import ast
import json
import logging
import ntpath
import os
import shutil
import time
import uuid
import filecmp
from datetime import datetime, timedelta
from distutils.dir_util import copy_tree, mkpath
from logging import log
from urllib.parse import urljoin

import requests
from amclient import AMClient
from bagit_create import main as bic
from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.settings import (
    AIP_UPSTREAM_BASEPATH,
    AM_ABS_DIRECTORY,
    AM_API_KEY,
    AM_REL_DIRECTORY,
    AM_SS_API_KEY,
    AM_SS_URL,
    AM_SS_USERNAME,
    AM_TRANSFER_SOURCE,
    AM_URL,
    AM_USERNAME,
    BIC_UPLOAD_PATH,
    FILES_URL,
    INVENIO_API_TOKEN,
    INVENIO_SERVER_URL,
    SIP_UPSTREAM_BASEPATH,
)
from oais_utils.validate import validate_sip, get_manifest
from oais_platform.oais.sources import InvalidSource, get_source

logger = get_task_logger(__name__)

# Execution flow


def finalize(self, status, retval, task_id, args, kwargs, einfo):
    """
    `Callback` for Celery tasks, handling result and updating
    the Archive

    status: Celery task status
    retval: returned value from the execution of the celery task
    task_id: Celery task ID
    args:
    """

    # ID of the Archive this Step is in
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)

    # ID of the Step this task was spawned for
    step_id = args[1]
    step = Step.objects.get(pk=step_id)

    # Should be removed?
    step.set_task(self.request.id)

    # If the Celery task succeded
    if status == states.SUCCESS:
        # This is for tasks failing without throwing an exception
        # (e.g BIC returning an error)
        if retval["status"] == 0:

            # Set last_step to the successful step
            archive.set_step(step)

            # Set step as completed and save finish date and output data
            step.set_status(Status.COMPLETED)
            step.set_finish_date()
            if not step.name == 5:
                step.set_output_data(retval)

            # If harvest or upload is completed then add the audit of the sip.json to the archive.manifest field
            if step.name == 2 or step.name == 1 or step.name == 8:
                sip_folder_name = archive.path_to_sip
                sip_manifest_path = "data/meta/sip.json"
                sip_location = os.path.join(sip_folder_name, sip_manifest_path)
                try:
                    with open(sip_location) as json_file:
                        sip_json = json.load(json_file)
                        ##TODO: Decide which part of the sip.json will go here
                        json_audit = sip_json["audit"]
                        archive.set_archive_manifest(json_audit)
                        logging.info(f"Sip.json audit saved at manifest field")
                except:
                    logging.info(f"Sip.json was not found inside {sip_location}")

            # Update the next possible steps
            next_steps = archive.update_next_steps(step.name)

            if len(next_steps) == 1:
                create_step(next_steps[0], archive_id, step_id)
        else:
            step.set_status(Status.FAILED)
            step.set_output_data(retval)
    else:
        step.set_status(Status.FAILED)


def run_next_step(archive_id, step_id):
    """
    Prepare a step for the given Archive
    """

    archive = Archive.objects.get(pk=archive_id)
    step_name = archive.next_steps[0]

    create_step(step_name, archive_id, step_id)


def create_step(step_name, archive_id, input_step_id=None):
    """
    Given a step name, create a new Step for the given
    Archive and spawn Celery tasks for it
    """

    try:
        input_step = Step.objects.get(pk=input_step_id)
        input_data = input_step.output_data
    except Exception:
        input_step = None
        input_data = None

    archive = Archive.objects.get(pk=archive_id)

    step = Step.objects.create(
        archive=archive,
        name=step_name,
        input_step=input_step,
        input_data=input_data,
        status=Status.WAITING,
    )

    # Consider switching this to "eval"?
    if step_name == Steps.HARVEST:
        task = process.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.VALIDATION:
        task = validate.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.CHECKSUM:
        task = checksum.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.ARCHIVE:
        task = archivematica.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.INVENIO_RDM_PUSH:
        task = invenio.delay(step.archive.id, step.id, step.input_data)

    return step


def create_path_artifact(name, path):
    """
    Given a step, the name and the path artifact and the description,
    """
    # If the path starts with a slash (e.g. in case of /eos/.. paths)
    #  remove it so we can join it without losing parts of the FILES_URL
    if path[0] == "/":
        non_abs_path = path[1:]

    url = urljoin(FILES_URL, non_abs_path)

    return {
        "artifact_name": name,
        "artifact_path": path,
        "artifact_url": url,
    }


# Steps implementations
@shared_task(
    name="processInvenio", bind=True, ignore_result=True, after_return=finalize
)
def invenio(self, archive_id, step_id, input_data=None):

    logger.info(f"Starting the publishing to InvenioRDM of Archive {archive_id}")

    archive = Archive.objects.get(pk=archive_id)

    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    headers = {
        "Authorization": "Bearer " + INVENIO_API_TOKEN,
        "Content-type": "application/json",
    }

    if archive.restricted is True:
        access = "private"
    else:
        access = "public"

    if not archive.creator.last_name:
        last_name = "Lopez"
    else:
        last_name = archive.creator.last_name

    if not archive.creator.first_name:
        first_name = "Sergio"
    else:
        first_name = archive.creator.first_name

    data = {
        "access": {
            "record": access,
            "files": access,
        },
        # Metadata only
        "files": {"enabled": False},
        "metadata": {
            "creators": [
                {
                    "person_or_org": {
                        "family_name": last_name,
                        "given_name": first_name,
                        "type": "personal",
                    }
                }
            ],
            "publication_date": "2018/2020-09",
            "resource_type": {"id": "image-photo"},
            "title": archive.title,
        },
    }
    # Create a record as a InvenioRDM
    invenio_records_endpoint = f"{INVENIO_SERVER_URL}/api/records"
    req = requests.post(
        invenio_records_endpoint, headers=headers, data=json.dumps(data), verify=False
    )

    data = json.loads(req.text)
    id_invenio = data["id"]
    relative_path = f"/records/{id_invenio}"

    # Create a InvenioRDM path artifact
    output_invenio_artifact = {
        "artifact_name": "Invenio Link",
        "artifact_path": "test",
        "artifact_url": f"{INVENIO_SERVER_URL}{relative_path}",
    }

    # Publish the InvenioRDM draft
    requests.post(
        f"{invenio_records_endpoint}/{id_invenio}/draft/actions/publish",
        headers=headers,
        verify=False,
    )

    return {"status": 0, "id": data["id"], "artifact": output_invenio_artifact}


@shared_task(name="process", bind=True, ignore_result=True, after_return=finalize)
def process(self, archive_id, step_id, input_data=None):
    """
    Run BagIt-Create to harvest data from upstream, preparing a
    Submission Package (SIP)
    """
    logger.info(f"Starting harvest of archive {archive_id}")

    archive = Archive.objects.get(pk=archive_id)

    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    api_token = None

    if archive.source == "indico":
        try:
            user = archive.creator
            api_token = user.profile.indico_api_key
        except Exception as e:
            return {"status": 1, "errormsg": e}

    if archive.source == "codimd":
        try:
            user = archive.creator
            api_token = user.profile.codimd_api_key
        except Exception as e:
            return {"status": 1, "errormsg": e}

    try:
        bagit_result = bic.process(
            recid=archive.recid,
            source=archive.source,
            loglevel=2,
            target=BIC_UPLOAD_PATH,
            token=api_token,
        )
    except Exception as e:
        return {"status": 1, "errormsg": e}

    logger.info(bagit_result)

    # If bagit returns an error return the error message
    if bagit_result["status"] == 1:
        return {"status": 1, "errormsg": bagit_result["errormsg"]}

    sip_folder_name = bagit_result["foldername"]

    if BIC_UPLOAD_PATH:
        sip_folder_name = os.path.join(BIC_UPLOAD_PATH, sip_folder_name)

    archive.set_path(sip_folder_name)

    # Create a SIP path artifact
    output_artifact = create_path_artifact(
        "SIP", os.path.join(SIP_UPSTREAM_BASEPATH, sip_folder_name)
    )

    bagit_result["artifact"] = output_artifact

    return bagit_result


@shared_task(name="validate", bind=True, ignore_result=True, after_return=finalize)
def validate(self, archive_id, step_id, input_data):

    archive = Archive.objects.get(pk=archive_id)
    sip_folder_name = archive.path_to_sip

    logger.info(f"Starting SIP validation {sip_folder_name}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    # Set task id
    current_step.set_task(self.request.id)

    # Checking registry = checking if the folder exists
    sip_exists = os.path.exists(sip_folder_name)

    if not sip_exists:
        return {"status": 1, "errormsg": "SIP does not exist"}

    # Runs validate_sip from oais_utils
    valid = validate_sip(sip_folder_name)

    return {"status": 0, "errormsg": None, "foldername": sip_folder_name}


@shared_task(name="checksum", bind=True, ignore_result=True, after_return=finalize)
def checksum(self, archive_id, step_id, input_data):
    archive = Archive.objects.get(pk=archive_id)
    path_to_sip = archive.path_to_sip

    logger.info(f"Starting checksum validation {path_to_sip}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    # Set task id
    current_step.set_task(self.request.id)

    sip_exists = os.path.exists(path_to_sip)
    if not sip_exists:
        return {"status": 1, "errormsg": "SIP does not exist"}

    sip_json = os.path.join(path_to_sip, "data/meta/sip.json")

    with open(sip_json) as json_file:
        data = json.load(json_file)
        for file in data["contentFiles"]:
            try:
                checksum_list = []
                for checksum in file["checksum"]:
                    splited = checksum.split(":")
                    checksum = splited[0] + ":" + "0"
                    checksum_list.append(checksum)
            except Exception:
                if (
                    file["origin"]["filename"] == "bagitcreate.log"
                    or file["origin"]["filename"] == "sip.json"
                ):
                    pass
                else:
                    return {"status": 1}

    logger.info(f"Checksum completed!")

    return {"status": 0, "errormsg": None, "foldername": path_to_sip}


@shared_task(
    name="archivematica",
    bind=True,
    ignore_result=True,
)
def archivematica(self, archive_id, step_id, input_data):
    """
    Gets the current step_id and the path to the sip folder and calls sends the sip to archivematica
    """
    archive = Archive.objects.get(pk=archive_id)
    path_to_sip = archive.path_to_sip

    logger.info(f"Starting archiving {path_to_sip}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    archive_id = current_step.archive

    # Set task id
    current_step.set_task(self.request.id)

    # This is the absolute directory of the archivematica-sampledata folder in the system
    a3m_abs_directory = AM_ABS_DIRECTORY
    # This is the directory Archivematica "sees" on the local system
    a3m_rel_directory = AM_REL_DIRECTORY

    # Get the destination folder of the system
    system_dst = os.path.join(
        a3m_abs_directory,
        ntpath.basename(path_to_sip),
    )

    # Get the destination folder of archivematica
    archivematica_dst = os.path.join(
        a3m_rel_directory,
        ntpath.basename(path_to_sip),
    )

    # Adds an _ between Archive and the id because archivematica messes up with spaces
    transfer_name = ntpath.basename(path_to_sip) + "::Archive_" + str(archive_id.id)

    # Get configuration from archivematica from settings
    am = AMClient()
    am.am_url = AM_URL
    am.am_user_name = AM_USERNAME
    am.am_api_key = AM_API_KEY
    am.transfer_source = AM_TRANSFER_SOURCE
    am.transfer_directory = archivematica_dst
    am.transfer_name = transfer_name
    am.processing_config = "automated"

    # Create archivematica package
    logging.info(
        f"Creating archivematica package on Archivematica instance: {AM_URL} at directory {archivematica_dst} for user {AM_USERNAME}"
    )

    try:
        # After 2 seconds check if the folder has been transfered to archivematica
        package = am.create_package()
        if package == 3:
            """
            In case there is an error in the request (Error 400, Error 404 etc),
            archivematica returns as a result the number 3. By filtering the result in that way,
            we know if am.create_package was executed successfully
            """
            logger.error(
                f"Error while archiving {current_step.id}. Check your archivematica settings configuration."
            )
            current_step.set_status(Status.FAILED)
            return {"status": 1, "errormsg": "Wrong Archivematica configuration"}

        step = Step.objects.get(pk=step_id)
        step.set_status(Status.WAITING)

        # Create the scheduler (sets every 10 seconds)
        schedule = IntervalSchedule.objects.create(
            every=5, period=IntervalSchedule.SECONDS
        )
        # Create a periodic task that checks the status of archivematica avery 10 seconds.
        PeriodicTask.objects.create(
            interval=schedule,
            name=f"Archivematica status for step: {current_step.id}",
            task="check_am_status",
            args=json.dumps([package, current_step.id, archive_id.id, transfer_name]),
            expires=timezone.now() + timedelta(minutes=600),
        )

    except Exception as e:
        logger.error(
            f"Error while archiving {current_step.id}. Check your archivematica settings configuration."
        )
        current_step.set_status(Status.FAILED)
        current_step.set_output_data({"status": 1, "errormsg": e})
        return {"status": 1, "errormsg": e}

    return {"status": 0, "errormsg": "Uploaded to Archivematica"}


@shared_task(
    name="check_am_status",
    bind=True,
    ignore_result=True,
)
def check_am_status(self, message, step_id, archive_id, transfer_name=None):

    step = Step.objects.get(pk=step_id)
    task_name = f"Archivematica status for step: {step_id}"

    # Get the current configuration
    am = AMClient()
    am.am_url = AM_URL
    am.am_user_name = AM_USERNAME
    am.am_api_key = AM_API_KEY
    am.transfer_source = AM_TRANSFER_SOURCE
    am.ss_url = AM_SS_URL
    am.ss_user_name = AM_SS_USERNAME
    am.ss_api_key = AM_SS_API_KEY

    try:
        periodic_task = PeriodicTask.objects.get(name=task_name)
        am_status = {"status": "PROCESSING", "microservice": "Waiting for upload"}

        try:
            am_status = am.get_unit_status(message["id"])
        except TypeError as e:
            if message == 1:
                """
                In case archivematica is not connected (Error 500, Error 502 etc),
                archivematica returns as a result the number 1. By filtering the result in that way,
                we know if am.get_unit_status was executed successfully
                """
                step.set_output_data({"status": 1, "errormsg": e})
                step.set_status(Status.FAILED)
                periodic_task = PeriodicTask.objects.get(name=task_name)
                periodic_task.delete()

            if message == 3:
                """
                In case there is an error in the request (Error 400, Error 404 etc),
                archivematica returns as a result the number 3. By filtering the result in that way,
                we know if am.get_unit_status was executed successfully
                """
                step.set_output_data({"status": 1, "errormsg": e})
                step.set_status(Status.FAILED)
                periodic_task = PeriodicTask.objects.get(name=task_name)
                periodic_task.delete()

            if step.status == Status.NOT_RUN:
                # As long as the package is in queue to upload get_unit_status returns nothing so a mock response is passed
                am_status = {
                    "status": "PROCESSING",
                    "microservice": "Waiting for upload",
                    "path": "",
                    "directory": "",
                    "name": "Pending...",
                    "uuid": "Pending...",
                    "message": "Waiting for upload to Archivematica",
                }

            logger.warning("Error while checking archivematica status: ", e)

        status = am_status["status"]
        microservice = am_status["microservice"]

        logger.info(f"Status for {step_id} is: {status}")

        # Needs to validate both because just status=complete does not guarantee that aip is stored
        if status == "COMPLETE" and microservice == "Remove the processing directory":
            """
            Archivematica does not return the uuid of a package AIP so in order to find the AIP details we need to look to all the AIPs and find
            the one with the same name. This way we can get the uuid and the path which are needed to access the AIP file
            """
            # Changes the :: to __ because archivematica by default does this transformation and this is needed so we can read the correct file
            transfer_name_with_underscores = transfer_name.replace("::", "__")

            aip_path = None
            aip_uuid = None

            aip_list = am.aips()  # Retrieves all the AIPs (needs AM_SS_* configuration)
            path_artifact = None
            for aip in aip_list:
                # Looks for aips with the same transfer name
                if transfer_name_with_underscores in aip["current_path"]:
                    aip_path = aip["current_path"]
                    aip_uuid = aip["uuid"]

                    am_status["aip_uuid"] = aip_uuid
                    am_status["aip_path"] = aip_path

                    path_artifact = create_path_artifact(
                        "AIP", os.path.join(AIP_UPSTREAM_BASEPATH, aip_path)
                    )

            # If the path artifact is found return complete otherwise set in progress and try again
            if path_artifact:
                am_status["artifact"] = path_artifact

                finalize(
                    self=self,
                    status=states.SUCCESS,
                    retval={"status": 0},
                    task_id=None,
                    args=[archive_id, step_id],
                    kwargs=None,
                    einfo=None,
                )

                step.set_finish_date()
                step.set_status(Status.COMPLETED)

                periodic_task = PeriodicTask.objects.get(name=task_name)
                periodic_task.delete()
            else:
                step.set_status(Status.IN_PROGRESS)

        elif status == "FAILED" and microservice == "Move to the failed directory":
            step.set_status(Status.FAILED)

            periodic_task = PeriodicTask.objects.get(name=task_name)
            periodic_task.delete()

        elif status == "PROCESSING":
            step.set_status(Status.IN_PROGRESS)

        step.set_output_data(am_status)

    except Exception as e:
        logger.warning(
            f"Error while archiving {step.id}. Archivematica pipeline is full or settings configuration is wrong."
        )
        logger.warning(e)
        periodic_task = PeriodicTask.objects.get(name=task_name)
        periodic_task.delete()
        step.set_status(Status.FAILED)


def announce_sip(announce_path, creator):
    """
    Given a filesystem path and a user:

    Run the OAIS validation tool on passed path and verify it's a proper SIP
    If true, import the SIP into the platform
    """
    logger.info(f"Starting announce of {announce_path}. Checking if the path points to a valid SIP..")

    # Check if the folder exists
    #  this can fail also if we don't have access
    folder_exists = os.path.exists(announce_path)
    if not folder_exists:
        return {"status": 1, "errormsg": "Folder does not exist or the oais user has no access"}

    sip_folder_name = ntpath.basename(announce_path)

    # Validate the folder as a SIP
    try:
        valid = validate_sip(announce_path)
    except Exception as e:
        return {"status": 1, "errormsg": f"Couldn't validate the path as a SIP. {e}"}

    if valid:
        try:
            sip_json = get_manifest(announce_path)
            source = sip_json["source"]
            recid = sip_json["recid"]
            if source != "local":
                url = get_source(source).get_record_url(recid)
            else:
                url = " "
        except Exception:
            return {"status": 1, "errormsg": "Error while reading sip.json"}

        # Create a new Archive
        archive = Archive.objects.create(
            recid=recid,
            source=source,
            source_url=url,
            creator=creator,
            title=f"{source} - {recid}"
        )

        # Create the starting Announce step
        step = Step.objects.create(
            archive=archive, name=Steps.ANNOUNCE, status=Status.IN_PROGRESS
        )

        output_data = {"foldername": sip_folder_name, "announce_path": announce_path}

        # Let's copy the SIP to our storage
        copy_sip.delay(archive.id, step.id, output_data)
        return {"status": 0, "archive_id": archive.id}

    else:
        return {"status": 1, "errormsg": "The given path is not a valid SIP."}


@shared_task(name="announce", bind=True, ignore_result=True, after_return=finalize)
def copy_sip(self, archive_id, step_id, input_data):
    """
    Given a path, copy the given path into the platform SIP storage
    If successful, save the final path in the passed Archive
    """

    foldername = input_data["foldername"]
    announce_path = input_data["announce_path"]

    if BIC_UPLOAD_PATH:
        target_path = os.path.join(BIC_UPLOAD_PATH, foldername)
    else:
        target_path = foldername
    try:
        os.mkdir(target_path)
    except FileExistsError:
        return {"status": 1, "errormsg": "The SIP couldn't be copied to the platform \
            because it already exists in the target destination."}
    try:
        for (dirpath, dirnames, filenames) in os.walk(announce_path, followlinks=False):
            logger.info(f"Starting copy of {announce_path} to {target_path}..")
            if announce_path == dirpath:
                target = target_path
            else:
                dest_relpath = dirpath[len(announce_path) + 1:]
                target = os.path.join(target_path, dest_relpath)
                os.mkdir(target)
            for file in filenames:
                shutil.copy(f"{os.path.abspath(dirpath)}/{file}", target)

        logger.info("Copy completed!")

        # Save the final target path
        archive = Archive.objects.get(pk=archive_id)
        archive.set_path(target_path)

        return {"status": 0, "errormsg": None, "foldername": foldername}

    except Exception as e:
        # In case of exception delete the target folder
        shutil.rmtree(target_path)
        return {"status": 1, "errormsg": e}
