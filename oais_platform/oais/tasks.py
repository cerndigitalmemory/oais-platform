import json
import logging
import ntpath
import os
import shutil
import time
from datetime import timedelta
from urllib.parse import urljoin

import bagit_create
import requests
from amclient import AMClient
from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from oais_utils.validate import get_manifest, validate_sip

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.sources import get_source
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
    BASE_URL,
    BIC_UPLOAD_PATH,
    FILES_URL,
    FTS_GRID_CERT,
    FTS_GRID_CERT_KEY,
    FTS_INSTANCE,
    INVENIO_API_TOKEN,
    INVENIO_SERVER_URL,
    SIP_UPSTREAM_BASEPATH,
)

from .fts import FTS

# Get the version of BagIt Create in use
bic_version = bagit_create.version.get_version()

# Set up logging
## Logger to be used inside Celery tasks
logger = get_task_logger(__name__)
## Standard logger
logging.basicConfig(level=logging.INFO)

try:
    # Get the FTS client ready
    fts = FTS(FTS_INSTANCE, FTS_GRID_CERT, FTS_GRID_CERT_KEY)
except Exception:
    logging.warning("Couldn't initialize the FTS client")


def finalize(self, status, retval, task_id, args, kwargs, einfo):
    """
    This "callback" function is called everytime a Celery task
    finished its execution to update the status of the
    relevant Archive and Step.

    status: Celery task status
    retval: returned value from the execution of the celery task
    task_id: Celery task ID
    """

    # ID of the Archive this Step is in
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)

    # ID of the Step this task was spawned for
    step_id = args[1]
    step = Step.objects.get(pk=step_id)

    # TODO: Check if this should be removed
    step.set_task(self.request.id)

    # If the Celery task succeded
    if status == states.SUCCESS:
        # Even if the status is SUCCESS, the task may have failed
        # (e.g. without throwing an exception) so here we check
        # for returned errors
        if retval["status"] == 0:
            # Set last_step to the successful step
            archive.set_step(step)

            # Set step as completed and save finish date and output data
            step.set_status(Status.COMPLETED)
            step.set_finish_date()
            if not step.name == 5:
                step.set_output_data(retval)

            # If harvest, upload or announce is completed then add the audit of the sip.json to the
            #  archive.manifest field
            if step.name in [1, 2, 8]:
                sip_folder_name = archive.path_to_sip
                sip_manifest_path = "data/meta/sip.json"
                sip_location = os.path.join(sip_folder_name, sip_manifest_path)
                try:
                    with open(sip_location) as json_file:
                        sip_json = json.load(json_file)
                        # Populate some values in the Archive model from the SIP manifest
                        # TODO: should other values be extracted ?
                        # Save the audit log from the sip.json
                        json_audit = sip_json["audit"]
                        archive.set_archive_manifest(json_audit)
                        logging.info("Sip.json audit saved at manifest field")
                except Exception:
                    logging.info(f"Sip.json was not found inside {sip_location}")

            # Update the next possible steps
            next_steps = archive.update_next_steps(step.name)

            # Automatically run next step ONLY if next_steps length is one (only one possible following step)
            # and current step is UPLOAD, HARVEST, CHECKSUM, VALIDATE or ANNOUNCE
            if len(next_steps) == 1 and step.name in [1, 2, 3, 8]:
                create_step(next_steps[0], archive_id, step_id)
        else:
            # Set the Step as failed and save the return value as the output data
            step.set_status(Status.FAILED)
            step.set_output_data(retval)
    else:
        step.set_status(Status.FAILED)


def run_next_step(archive_id, previous_step_id):
    """
    Given an Archive (and its last executed step),
    create the next step in the pipeline,
    selecting the first possible one in the pipeline definition
    """

    archive = Archive.objects.get(pk=archive_id)
    step_name = archive.next_steps[0]

    create_step(step_name, archive_id, previous_step_id)


def create_step(step_name, archive_id, input_step_id=None):
    """
    Create a new Step of the desired type
    for the given Archive and spawn Celery tasks for it

    step_name: type of the step
    archive_id: ID of the target Archive
    input_step_id: (optional) step to set as "input" for the new one
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

    # TODO: Consider switching this to "eval" or something similar
    if step_name == Steps.HARVEST:
        process.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.VALIDATION:
        validate.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.CHECKSUM:
        checksum.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.ARCHIVE:
        archivematica.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.INVENIO_RDM_PUSH:
        invenio.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.PUSH_SIP_TO_CTA:
        push_sip_to_cta.delay(step.archive.id, step.id, step.input_data)

    return step


def create_path_artifact(name, path, localpath):
    """
    Serialize an "Artifact" object with the given values.
    The "URL" path is built prefixing the FILES_URL setting
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


@shared_task(name="push_sip_to_cta", bind=True, ignore_result=True)
def push_sip_to_cta(self, archive_id, step_id, input_data=None):
    """
    Push the SIP of the given Archive to CTA, preparing the FTS Job,
    locations etc, then saving the details of the operation as the output
    artifact. Once done, set up another periodic task to check on
    the status of the transfer.
    """
    logger.info(f"Pushing Archive {archive_id} to CTA")

    # Get the Archive and Step we're running for
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    path_to_sip = archive.path_to_sip
    # And set the step as in progress
    step.set_status(Status.IN_PROGRESS)

    cta_folder_name = f"sip-{archive.id}-{int(time.time())}"

    submitted_job = fts.push_to_cta(
        f"root://eospublic.cern.ch/{path_to_sip}",
        f"root://eosctapublicpps.cern.ch//eos/ctapublicpps/archivetest/digital-memory/test/{cta_folder_name}",
    )

    logger.info(submitted_job)

    output_cta_artifact = {
        "artifact_name": "FTS Job",
        "artifact_path": cta_folder_name,
        "artifact_url": f"https://fts3-pilot.cern.ch:8449/fts3/ftsmon/#/job/{submitted_job}",
    }

    # Create the scheduler
    schedule = IntervalSchedule.objects.create(every=2, period=IntervalSchedule.SECONDS)
    # Spawn a periodic task to check for the status of the job
    PeriodicTask.objects.create(
        interval=schedule,
        name=f"FTS job status for step: {step.id}",
        task="check_fts_job_status",
        args=json.dumps([archive.id, step.id, submitted_job]),
        expires=timezone.now() + timedelta(minutes=600),
    )

    step.set_output_data(
        {"status": 0, "artifact": output_cta_artifact, "fts_job_id": submitted_job}
    )


@shared_task(name="check_fts_job_status", bind=True, ignore_result=True)
def check_fts_job_status(self, archive_id, step_id, job_id):
    """
    Check the status of a FTS job.
    If finished, set the corresponding step as completed and remove the
    periodic task.
    """
    step = Step.objects.get(pk=step_id)
    status = fts.job_status(job_id)

    task_name = f"FTS job status for step: {step.id}"
    periodic_task = PeriodicTask.objects.get(name=task_name)

    if status["job_state"] == "FINISHED":
        logger.info("Looks like the transfer succeded, removing periodic task")

        step.set_finish_date()
        step.set_status(Status.COMPLETED)

        periodic_task = PeriodicTask.objects.get(name=task_name)
        periodic_task.delete()

    logger.info(status["job_state"])


@shared_task(
    name="processInvenio", bind=True, ignore_result=True, after_return=finalize
)
def invenio(self, archive_id, step_id, input_data=None):
    """
    Publish an Archive on the configured InvenioRDM instance
    If the Archive was already published, create a new version of the Record.
    If another Archive referring to the same Resource (Source, Record ID)
    was already published, create a new version of the Record.
    """
    logger.info(f"Starting the publishing to InvenioRDM of Archive {archive_id}")

    # Get the Archive and Step we're running for
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    # And set the step as in progress
    step.set_status(Status.IN_PROGRESS)

    # The InvenioRDM API endpoint
    invenio_records_endpoint = f"{INVENIO_SERVER_URL}/api/records"

    # Set up the authentication headers for the requests to the InvenioRDM API
    headers = {
        "Authorization": "Bearer " + INVENIO_API_TOKEN,
        "Content-type": "application/json",
    }

    # If this Archive was never published before to InvenioRDM
    # and no similar Archive was published before

    if (archive.resource.invenio_parent_id) is None:
        # We create a brand new Record in InvenioRDM
        archive.invenio_version = 1
        data = prepare_invenio_payload(archive)

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

        # An InvenioRDM parent ID groups every published version reffering to the same Resource
        data_published = json.loads(req_publish_invenio.text)
        invenio_parent_id = data_published["parent"]["id"]

        # Save the Invenio parent ID on the Resource
        resource = archive.resource
        resource.set_invenio_id(invenio_id)
        resource.set_invenio_parent_fields(invenio_parent_id)

        # Save the resource and the archive
        resource.save()
        archive.save()

    # Create a new InvenioRDM version of an already published Record
    else:
        # Let's get the Parent ID for which we will create a new version
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
        new_version_data = prepare_invenio_payload(archive)

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
    logger.info(
        f"Starting harvest of Archive {archive_id} using BagIt Create {bic_version}"
    )

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
        bagit_result = bagit_create.main.process(
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
    """
    Validate the a folder against the CERN SIP specification,
    using the OAIS utils package
    """
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
    validate_sip(sip_folder_name)

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
            except KeyError:
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
    Submit the SIP of the passed Archive to Archivematica
    preparing the call to the Archivematica API
    Once done, spawn a periodic task to check on the progress
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

    # Set up the AMClient to interact with the AM configuration provided in the settings
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
        if package in [-1, 1, 2, 3, 4]:
            """
            The AMClient will return values in [-1, 1, 2, 3, 4] when there was an error in the request to the AM API.
            We can't do much in these cases, a part from suggesting to take a look at the AM logs.
            Check 'amclient/errors' for more information.
            """
            logger.error(
                f"Error while archiving {current_step.id}. Check your archivematica settings configuration."
            )
            current_step.set_status(Status.FAILED)
            errormsg = f"AM Create package returned {package}. This may be a configuration error. Check AM logs for more information."
            current_step.set_output_data({"status": 1, "errormsg": errormsg})
            return {"status": 1, "errormsg": errormsg}
        else:
            step = Step.objects.get(pk=step_id)
            step.set_status(Status.WAITING)

            # Create the scheduler
            schedule = IntervalSchedule.objects.create(
                every=60, period=IntervalSchedule.SECONDS
            )
            # Spawn a periodic task to check for the status of the package on AM
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
                f"Error while archiving {current_step.id} (403). Check your archivematica credentials."
            )
            current_step.set_status(Status.FAILED)
            current_step.set_output_data(
                {"status": 1, "errormsg": "Check your archivematica credentials (403)."}
            )
            return {
                "status": 1,
                "errormsg": "Check your archivematica credentials (403).",
            }
        else:
            logger.error(
                f"Error while archiving {current_step.id} ({e.request.status_code}). Check your archivematica settings configuration."
            )
            current_step.set_status(Status.FAILED)
            current_step.set_output_data(
                {
                    "status": 1,
                    "errormsg": "Check your archivematica settings configuration. ({e.request.status_code})",
                }
            )
            return {
                "status": 1,
                "errormsg": "Check your archivematica settings configuration. ({e.request.status_code})",
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
    """
    Check the status of an Archivematica job by polling its API.
    The related Step is updated with the information returned from Archivematica
    e.g. the current microservice running or the final result.
    """
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

        elif status == "PROCESSING":
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
    Set step as failed and remove the scheduled task
    """
    step.set_status(Status.FAILED)
    logger.warning(f"Step {step.id} failed. Step status: {step.status}")
    periodic_task.delete()


def prepare_invenio_payload(archive):
    """
    From the Archive data and metadata, prepare the payload to create an Invenio Record,
    ready to be POSTed to the Invenio RDM API.
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

    # We don't have reliable information about the authors of the upstream resource here,
    # so let's put a placeholder
    last_name = "N/A"
    first_name = "N/A"

    # Prepare the artifacts to publish
    # Get all the completed (status = 4) steps of the Archive
    steps = archive.steps.all().order_by("start_date").filter(status=4)

    invenio_artifacts = []

    for step in steps:
        if "artifact" in step.output_data:
            out_data = json.loads(step.output_data)
            if out_data["artifact"]["artifact_name"] in ["SIP", "AIP"]:
                invenio_artifacts.append(
                    {
                        "type": out_data["artifact"]["artifact_name"],
                        "link": f"{BASE_URL}/api/steps/{step.id}/download-artifact",
                        "path": out_data["artifact"]["artifact_path"],
                        "add_details": {
                            "SIP": "Submission Information Package as harvested by the platform from the upstream digital repository.",
                            "AIP": "Archival Information Package, as processed by Archivematica.",
                        }[out_data["artifact"]["artifact_name"]],
                        "timestamp": step.finish_date.strftime("%m/%d/%Y, %H:%M:%S"),
                    }
                )

    # Prepare the final payload
    data = {
        "access": {
            "record": access,
            "files": access,
        },
        # Set it as Metadata only
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
            # The version "name" we give on invenio is the Nth time we publish to invenio + the Archive ID from the platform
            # (there can be different Archive IDs going as a version to the same Invenio record: when two Archives are about the same Resource)
            "version": f"{archive.invenio_version}, Archive {archive.id}",
        },
        "custom_fields": {"artifacts": invenio_artifacts},
    }

    return data


def announce_sip(announce_path, creator):
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

    if valid:
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
            creator=creator,
            title=f"{source} - {recid}",
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
    Given a path, copy it into the platform SIP storage
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


# TODO: Do we need input_data parameter for this function?
@shared_task(name="download_files", bind=True, ignore_result=True)
def download_files(self, archive_id, step_id):
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    
    # TODO: Do we need to set Status to IN_PROGRESS?
    # step.set_status(Status.IN_PROGRESS)

    data = json.loads(step.input_data)
    number_of_downloaded_files = 0

    # Creates a subfolder needed for file storage
    folder_name = os.path.join(LOCAL_BASE_PATH, step.output_data)
    os.makedirs(folder_name, exist_ok=True)

    for name, url in data.items():
        if number_of_downloaded_files == FILE_LIMIT:
            # TODO: Check what to do if user sends number of files that exceeds the FILE_LIMIT
            return False

        response = requests.get(url)
        
        with open(os.path.join(folder_name, name), "wb") as file:
            file.write(response.content)
            number_of_downloaded_files += 1

    return True
