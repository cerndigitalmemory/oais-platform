import ntpath
import os
import shutil

from celery import shared_task
from celery.utils.log import get_task_logger
from django.contrib.auth.models import User
from oais_utils.validate import get_manifest, validate_sip

from oais_platform.oais.models import Archive, Collection, Status, Step, StepName
from oais_platform.oais.sources.utils import get_source
from oais_platform.oais.tasks.pipeline_actions import finalize, run_step
from oais_platform.oais.tasks.utils import (
    add_error_to_tag_description,
    create_path_artifact,
    create_step,
)
from oais_platform.settings import BIC_UPLOAD_PATH, SIP_UPSTREAM_BASEPATH

logger = get_task_logger(__name__)


def announce_sip(announce_path, user):
    """
    Given a filesystem path and a user:

    Run the OAIS validation tool on passed path and verify it's a proper SIP
    If true, import the SIP into the platform, creating an Archive for it
    and setting the first Step
    """
    logger.info(
        f"Starting announce of {announce_path}. Checking if the path points to a valid SIP.."
    )

    # Check if the folder exists
    #  this can fail also if we don't have access
    folder_exists = os.path.exists(announce_path)
    if not folder_exists:
        return {
            "status": 1,
            "errormsg": "Folder does not exist or the oais user has no access",
        }

    sip_folder_name = ntpath.basename(announce_path)

    # Validate the folder as a SIP
    try:
        valid = validate_sip(announce_path)
    except Exception as e:
        return {"status": 1, "errormsg": f"Couldn't validate the path as a SIP. {e}"}

    if not valid:
        return {"status": 1, "errormsg": "The given path is not a valid SIP"}

    try:
        sip_json = get_manifest(announce_path)
        source = sip_json["source"]
        recid = sip_json["recid"]
        try:
            if source != "local":
                url = get_source(source).get_record_url(recid)
            else:
                url = "N/A"
        except Exception:
            url = "N/A"
    except Exception:
        return {"status": 1, "errormsg": "Error while reading sip.json"}

    # Create a new Archive
    archive = Archive.objects.create(
        recid=recid,
        source=source,
        source_url=url,
        approver=user,
        requester=user,
        title=f"{source} - {recid}",
    )

    # Create the starting Announce step
    input_data = {"foldername": sip_folder_name, "announce_path": announce_path}

    step = create_step(
        StepName.ANNOUNCE, archive, input_step_id=None, input_data=input_data, user=user
    )

    # Let's copy the SIP to our storage
    run_step(step, archive.id, api_key=None)
    return {"status": 0, "archive_id": archive.id}


@shared_task(name="announce", bind=True, ignore_result=True, after_return=finalize)
def copy_sip(self, archive_id, step_id, input_data, api_key=None):
    """
    Given a path, copy it into the platform SIP storage
    If successful, save the final path in the passed Archive
    """
    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    foldername = input_data["foldername"]
    announce_path = input_data["announce_path"]

    if BIC_UPLOAD_PATH:
        target_path = os.path.join(BIC_UPLOAD_PATH, foldername)
    else:
        target_path = foldername
    try:
        os.mkdir(target_path)
    except FileExistsError:
        return {
            "status": 1,
            "errormsg": "The SIP couldn't be copied to the platform \
            because it already exists in the target destination.",
        }
    try:
        for dirpath, dirnames, filenames in os.walk(announce_path, followlinks=False):
            logger.info(f"Starting copy of {announce_path} to {target_path}..")
            if announce_path == dirpath:
                target = target_path
            else:
                dest_relpath = dirpath[len(announce_path) + 1 :]
                target = os.path.join(target_path, dest_relpath)
                os.mkdir(target)
            for file in filenames:
                shutil.copy(f"{os.path.abspath(dirpath)}/{file}", target)

        logger.info("Copy completed!")

        # Save the final target path
        archive = Archive.objects.get(pk=archive_id)
        archive.set_path(target_path)

        # Create a SIP path artifact
        output_artifact = create_path_artifact(
            "SIP", os.path.join(SIP_UPSTREAM_BASEPATH, target_path), target_path
        )
        return {
            "status": 0,
            "errormsg": None,
            "foldername": foldername,
            "artifact": output_artifact,
        }

    except Exception as e:
        # In case of exception delete the target folder
        shutil.rmtree(target_path)
        return {"status": 1, "errormsg": e}


@shared_task(name="batch_announce_task", bind=True, ignore_result=True)
def batch_announce_task(self, announce_path, tag_id, user_id):
    # Run the "announce" procedure for every subfolder(validate, create an Archive, copy)
    user = User.objects.get(pk=user_id)
    tag = Collection.objects.get(pk=tag_id)

    for f in os.scandir(announce_path):
        try:
            if f.is_dir() and f.path != announce_path:
                announce_response = announce_sip(f.path, user)
                if announce_response["status"] == 0:
                    tag.add_archive(announce_response["archive_id"])
                else:
                    add_error_to_tag_description(
                        tag, f.path, announce_response["errormsg"]
                    )
        except Exception as e:
            add_error_to_tag_description(tag, f.path, f"Exception {str(e)}")

    tag.set_description(tag.description.replace("Batch Announce processing...", ""))
    if len(tag.description) == 0:
        tag.set_description("Batch Announce completed successfully")
