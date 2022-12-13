import ast
import json
import logging
import ntpath
import os
import shutil
import time
import uuid
from datetime import datetime, timedelta
from distutils.dir_util import copy_tree, mkpath
from lib2to3.pgen2.token import RPAR
from logging import log
from urllib.parse import urljoin
from urllib.error import HTTPError

import requests
from amclient import AMClient
from bagit_create import main as bic
from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from oais_platform.oais.models import Archive, Resource, Status, Step, Steps
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
from oais_utils.validate import validate_sip

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
            if step.name == 2 or step.name == 1:
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

            # Automatically run next step ONLY if next_steps length is one and
            # current step is UPLOAD, HARVEST, CHECKSUM or VALIDATE
            if len(next_steps) == 1 and step.name in [1, 2, 3, 4]:
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


def create_path_artifact(name, path, localpath):
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
        "artifact_localpath": localpath,
        "artifact_url": url,
    }


# Steps implementations
@shared_task(
    name="processInvenio", bind=True, ignore_result=True, after_return=finalize
)
def invenio(self, archive_id, step_id, input_data=None):
    """
    Publish an Archive on our platform as a new Record on the configured InvenioRDM instance.
    If the Archive was already published, create a new version of the Record.
    If another Archive referring to the same (Source, Record ID) was already published,
    create a new version of the Record.
    """
    logger.info(f"Starting the publishing to InvenioRDM of Archive {archive_id}")

    # Get the Archive and Step we're running for
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    # The InvenioRDM API endpoint
    invenio_records_endpoint = f"{INVENIO_SERVER_URL}/api/records"

    # Set up the authentication for the requests to the InvenioRDM API
    headers = {
        "Authorization": "Bearer " + INVENIO_API_TOKEN,
        "Content-type": "application/json",
    }

    # If this Archive was never published before to InvenioRDM
    # and no similar Archive was published before
    if (archive.resource.invenio_parent_id) is None:

        # We create a brand new Record in InvenioRDM
        archive.invenio_version = 1
        data = initialize_data(archive)

        try:

            # Create a record as a InvenioRDM draft
            req = requests.post(
                invenio_records_endpoint,
                headers=headers,
                data=json.dumps(data),
                verify=False,
            )
            req.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(f"The request didn't succed:{err}")
            step.set_status(Status.FAILED)
            return {"status": 1, "ERROR": err}

        # Parse the response and get our new record ID so we can link it
        data_loaded = json.loads(req.text)
        invenio_id = data_loaded["id"]
        relative_path = f"/records/{invenio_id}"

        # Create a path artifact with a link to the InvenioRDM Record we just created
        # FIXME: Use a single method to create artifacts
        output_invenio_artifact = {
            "artifact_name": "Invenio Link",
            "artifact_path": "test",
            "artifact_url": f"{INVENIO_SERVER_URL}{relative_path}",
        }

        # Publish the InvenioRDM draft so it's accessible publicly
        req_publish_invenio = requests.post(
            f"{invenio_records_endpoint}/{invenio_id}/draft/actions/publish",
            headers=headers,
            verify=False,
        )

        # An InvenioRDM parent ID groups every version of the same Record, extract it
        data_published = json.loads(req_publish_invenio.text)
        invenio_parent_id = data_published["parent"]["id"]

        # Set the value to the resource fields for the first time
        resource = archive.resource
        resource.set_invenio_id(invenio_id)
        resource.set_invenio_parent_fields(invenio_parent_id)

        # Save the resource and the archive
        resource.save()
        archive.save()

    # Create a new InvenioRDM version of an already published Record
    else:

        # Invenio_id of the first version published in InvenioRDM of the resource
        invenio_id = archive.resource.invenio_id

        # Create new version as draft
        req_invenio_draft_new_version = requests.post(
            f"{INVENIO_SERVER_URL}/api/records/{invenio_id}/versions",
            headers=headers,
            verify=False,
        )

        # Get the ID of the draft we just created
        new_version_invenio_id = json.loads(req_invenio_draft_new_version.text)["id"]

        # Increment the version
        archive.invenio_version += 1

        # Initialize the archive data that is going to be sent on the request
        new_version_data = initialize_data(archive)

        # Update draft with the new adata
        requests.put(
            f"{invenio_records_endpoint}/{new_version_invenio_id}/draft",
            headers=headers,
            data=json.dumps(new_version_data),
            verify=False,
        )

        # Publish the new Invenio RDM version draft
        requests.post(
            f"{invenio_records_endpoint}/{new_version_invenio_id}/draft/actions/publish",
            headers=headers,
            verify=False,
        )

        archive.save()

        # Create a InvenioRDM path artifact with a link to the new version
        # FIXME: Use a single method to create artifacts
        relative_path = f"/records/{new_version_invenio_id}"
        output_invenio_artifact = {
            "artifact_name": "Invenio Link",
            "artifact_path": "test",
            "artifact_url": f"{INVENIO_SERVER_URL}{relative_path}",
        }

    return {"status": 0, "id": invenio_id, "artifact": output_invenio_artifact}


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
        "SIP", os.path.join(SIP_UPSTREAM_BASEPATH, sip_folder_name), sip_folder_name
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
            except KeyError as e:
                current_file = file["origin"]["filename"]
                logger.info(f"Checksum not found for file {current_file}")

    logger.info("Checksum completed!")

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
        package = am.create_package()
        logging.info(f"Package {package} created successfully")
        if package == 3:
            """
            In case archivematica is not connected (Error 500, Error 502 etc),
            archivematica returns as a result the number 3. By filtering the result in that way,
            we know if am.get_unit_status was executed successfully
            """
            logger.error(
                f"Error while archiving {current_step.id}. Check your archivematica settings configuration."
            )
            current_step.set_status(Status.FAILED)
            current_step.set_output_data(
                {"status": 1, "errormsg": "Wrong Archivematica configuration"}
            )
            return {"status": 1, "errormsg": "Wrong Archivematica configuration"}
        elif package == 1:
            """
            In case there is an error in the request (Error 400, Error 404 etc),
            archivematica returns as a result the number 1. By filtering the result in that way,
            we know if am.get_unit_status was executed successfully
            """
            logger.error(
                f"Error while archiving {current_step.id}. Check your archivematica settings configuration."
            )
            current_step.set_status(Status.FAILED)
            current_step.set_output_data(
                {"status": 1, "errormsg": "Wrong Archivematica configuration"}
            )
            return {"status": 1, "errormsg": "Wrong Archivematica configuration"}
        else:
            step = Step.objects.get(pk=step_id)
            step.set_status(Status.WAITING)

            # Create the scheduler (sets every 10 seconds)
            schedule = IntervalSchedule.objects.create(
                every=60, period=IntervalSchedule.SECONDS
            )
            # Create a periodic task that checks the status of archivematica.
            PeriodicTask.objects.create(
                interval=schedule,
                name=f"Archivematica status for step: {current_step.id}",
                task="check_am_status",
                args=json.dumps(
                    [package, current_step.id, archive_id.id, transfer_name]
                ),
                expires=timezone.now() + timedelta(minutes=600),
            )
    except requests.HTTPError as e:
        if e.request.status_code == 403:
            """
            In case of error 403: Authentication issues (wrong credentials)
            """
            logger.error(
                f"Error while archiving {current_step.id}. Check your archivematica credentials."
            )
            current_step.set_status(Status.FAILED)
            current_step.set_output_data(
                {"status": 1, "errormsg": "Check your archivematica credentials."}
            )
            return {"status": 1, "errormsg": "Check your archivematica credentials."}
        else:
            logger.error(
                f"Error while archiving {current_step.id}. Check your archivematica settings configuration."
            )
            current_step.set_status(Status.FAILED)
            current_step.set_output_data(
                {
                    "status": 1,
                    "errormsg": "Check your archivematica settings configuration.",
                }
            )
            return {
                "status": 1,
                "errormsg": "Check your archivematica settings configuration.",
            }

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
            logging.info(f"Current unit status for {am_status}")
        except requests.HTTPError as e:
            logging.info(f"Error {e.response.status_code} for archivematica")
            if e.response.status_code == 400:
                if step.status == Status.WAITING:
                    # As long as the package is in queue to upload get_unit_status returns nothing so a mock response is passed
                    am_status = {
                        "status": "PROCESSING",
                        "microservice": "Waiting for archivematica to respond",
                        "path": "",
                        "directory": "",
                        "name": "Pending...",
                        "uuid": "Pending...",
                        "message": "Waiting for upload to Archivematica",
                    }
                    step.set_status(Status.IN_PROGRESS)
                    logging.info(f"Current unit status for {am_status}")
                else:
                    # If step status is not waiting, then archivematica delayed to respond so package creation is considered failed.
                    # This is usually because archivematica may not have access to the file or the transfer source is not correct.
                    am_status["status"] = "FAILED"
                    am_status["microsrver"] = "Archivematica delayed to respond."
                    step.set_output_data(
                        {
                            "status": 1,
                            "errormsg": "Archivematica did not respond to package creation. Check transfer file and transfer source configuration",
                        }
                    )
                    remove_periodic_task(periodic_task, step)
            else:
                # If there is other type of error code then archivematica connection could not be establissed.
                step.set_output_data(
                    {
                        "status": 1,
                        "errormsg": "Error: Could not connect to archivematica",
                    }
                )
                remove_periodic_task(periodic_task, step)
        except Exception as e:
            """
            In any other case make task fail (Archivematica crashed or not responding)
            """
            step.set_output_data({"status": 1, "errormsg": e})
            remove_periodic_task(periodic_task, step)

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
                        "AIP", os.path.join(AIP_UPSTREAM_BASEPATH, aip_path), aip_path
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
            remove_periodic_task(periodic_task, step)

        elif status == "PROCESSING" and "Waiting for upload":
            step.set_status(Status.IN_PROGRESS)

        step.set_output_data(am_status)

    except Exception as e:
        logger.warning(
            f"Error while archiving {step.id}. Archivematica pipeline is full or settings configuration is wrong."
        )
        logger.warning(e)
        remove_periodic_task(periodic_task, step)


def remove_periodic_task(periodic_task, step):
    """
    Sets step as failed and removes the scheduling task
    """
    step.set_status(Status.FAILED)
    logger.warning(f"Step {step.id} failed. Step status: {step.status}")
    periodic_task.delete()


def initialize_data(archive):
    """
    From the Archive, prepare some metadata to create the Invenio Record
    """

    # If there's no title, put the source and the record ID
    if archive.title == "":
        title = f"{archive.source} : {archive.recid}"
    else:
        title = archive.title

    if archive.restricted is True:
        access = "private"
    else:
        access = "public"

    if not archive.creator.last_name:
        last_name = "Smith"
    else:
        last_name = archive.creator.last_name

    if not archive.creator.first_name:
        first_name = "David"
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
            # Set publication_date to the moment we trigger a publish
            "publication_date": archive.timestamp.date().isoformat(),
            "resource_type": {"id": "publication"},
            "title": title,
            "description": f"<b>Source:</b> {archive.source}<br><b>Link:</b> <a href={archive.source_url}>{archive.source_url}<br></a>",
            # The first time we publish to InvenioRDM we call the version '1'
            "version": f"{archive.invenio_version}, Archive {archive.id}",
        },
    }

    return data
