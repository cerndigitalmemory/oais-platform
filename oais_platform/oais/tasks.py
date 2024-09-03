import json
import logging
import ntpath
import os
import shutil
import time
import xml.etree.ElementTree as ET
from urllib.parse import urljoin

import bagit_create
import requests
from amclient import AMClient
from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.contrib.auth.models import User
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from oais_utils.validate import get_manifest, validate_sip

from oais_platform.oais.models import Archive, Collection, Status, Step, Steps
from oais_platform.oais.sources.utils import get_source
from oais_platform.settings import (
    AIP_UPSTREAM_BASEPATH,
    AM_API_KEY,
    AM_REL_DIRECTORY,
    AM_SS_API_KEY,
    AM_SS_URL,
    AM_SS_USERNAME,
    AM_TRANSFER_SOURCE,
    AM_URL,
    AM_USERNAME,
    AM_WAITING_TIME_LIMIT,
    BASE_URL,
    BIC_UPLOAD_PATH,
    CTA_BASE_PATH,
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
            archive.set_last_completed_step(step, lock=True)

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

            # Execute the remainig steps in the pipeline
            execute_pipeline(archive)

        else:
            # Set the Step as failed and save the return value as the output data
            step.set_status(Status.FAILED)
            step.set_output_data(retval)
    else:
        step.set_status(Status.FAILED)


def run_next_step(archive, previous_step_id):
    """
    Given an Archive (and its last executed step),
    create the next step in the pipeline,
    selecting the first possible one in the pipeline definition
    """

    step_name = archive.get_next_steps(previous_step_id)[0]
    archive.add_step_to_pipeline(step_name, lock=True)
    execute_pipeline(archive)


def create_step(step_name, archive_id, input_step_id=None, api_key=None):
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
        process.delay(step.archive.id, step.id, api_key, input_data=step.input_data)
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
    elif step_name == Steps.EXTRACT_TITLE:
        extract_title.delay(archive.id, step.id)

    return step


def run_step(step_id, archive, api_key=None):

    step = Step.objects.get(pk=step_id)
    step.status = Status.WAITING

    try:
        input_step = step.input_step
        step.input_data = input_step.output_data
    except Exception:
        step.input_data = None

    archive.set_last_step(step, lock=True)

    if step.name == Steps.HARVEST:
        process.delay(step.archive.id, step.id, api_key, input_data=step.input_data)
    elif step.name == Steps.VALIDATION:
        validate.delay(step.archive.id, step.id, step.input_data)
    elif step.name == Steps.CHECKSUM:
        checksum.delay(step.archive.id, step.id, step.input_data)
    elif step.name == Steps.ARCHIVE:
        archivematica.delay(step.archive.id, step.id, step.input_data)
    elif step.name == Steps.INVENIO_RDM_PUSH:
        invenio.delay(step.archive.id, step.id, step.input_data)
    elif step.name == Steps.PUSH_SIP_TO_CTA:
        push_sip_to_cta.delay(step.archive.id, step.id, step.input_data)
    elif step.name == Steps.EXTRACT_TITLE:
        extract_title.delay(archive.id, step.id)

    return step


def execute_pipeline(archive, api_key=None):

    step_id = archive.consume_pipeline()

    # No Steps to execute in the pipelime
    if step_id is None:
        # Automatically run next step ONLY if next_steps length is one (only one possible following step)
        # and current step is UPLOAD, HARVEST, CHECKSUM, VALIDATE or ANNOUNCE
        last_completed_step = archive.last_completed_step
        next_steps = archive.get_next_steps()

        if len(next_steps) == 1 and last_completed_step.name in [1, 2, 3, 8]:
            archive.add_pipeline_step(next_steps[0])
            execute_pipeline(archive)
        else:
            return None

    return run_step(step_id, archive, api_key)


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
        f"root://eosuser.cern.ch/{path_to_sip}",
        f"{CTA_BASE_PATH}{cta_folder_name}",
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
        expire_seconds=2.0,
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
    elif status["job_state"] == "FAILED":
        step.set_finish_date()
        step.set_status(Status.FAILED)

        periodic_task = PeriodicTask.objects.get(name=task_name)
        periodic_task.delete()


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
            logger.error(f"The request didn't succed:{err}")
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
def process(self, archive_id, step_id, api_key=None, input_data=None):
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

    if not api_key:
        logger.info(
            f"The given source({archive.source}) might requires an API key which was not provided."
        )

    try:
        bagit_result = bagit_create.main.process(
            recid=archive.recid,
            source=archive.source,
            loglevel=2,
            target=BIC_UPLOAD_PATH,
            token=api_key,
        )
    except Exception as e:
        return {"status": 1, "errormsg": str(e)}

    logger.info(bagit_result)

    # If bagit returns an error return the error message
    if bagit_result["status"] == 1:
        return {"status": 1, "errormsg": str(bagit_result["errormsg"])}

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

    # This is the directory Archivematica "sees" on the local system
    a3m_rel_directory = AM_REL_DIRECTORY

    # Get the destination folder of archivematica
    archivematica_dst = os.path.join(
        a3m_rel_directory,
        ntpath.basename(path_to_sip),
    )

    # Adds an _ between Archive and the id because archivematica messes up with spaces
    transfer_name = ntpath.basename(path_to_sip) + "::Archive_" + str(archive_id.id)

    # Set up the AMClient to interact with the AM configuration provided in the settings
    am = _get_am_client()
    am.transfer_directory = archivematica_dst
    am.transfer_name = transfer_name

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
            current_step.set_status(Status.WAITING)

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
                expire_seconds=60.0,
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
                    "errormsg": f"Check your archivematica settings configuration. ({e.request.status_code})",
                }
            )
            return {
                "status": 1,
                "errormsg": f"Check your archivematica settings configuration. ({e.request.status_code})",
            }

    except Exception as e:
        logger.error(
            f"Error while archiving {current_step.id}. Check your archivematica settings configuration."
        )
        current_step.set_status(Status.FAILED)
        current_step.set_output_data({"status": 1, "errormsg": str(e)})
        return {"status": 1, "errormsg": str(e)}

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

    am = _get_am_client()

    try:
        periodic_task = PeriodicTask.objects.get(name=task_name)

        try:
            am_status = am.get_unit_status(message["id"])
            logging.info(f"Current unit status for {am_status}")
        except requests.HTTPError as e:
            logging.info(f"Error {e.response.status_code} for archivematica")
            if e.response.status_code == 400:
                is_failed = True
                try:
                    # It is possible that the package is in queue between transfer and ingest - in this case it returns 400 but there are executed jobs
                    am.unit_uuid = message["id"]
                    executed_jobs = am.get_jobs()
                    logging.debug(
                        f"Executed jobs for given id({message['id']}): {executed_jobs}"
                    )
                    if executed_jobs != 1 and len(executed_jobs) > 0:
                        is_failed = False
                        am_status = {
                            "status": "PROCESSING",
                            "microservice": "Waiting for archivematica to continue the processing",
                        }
                        logging.info(
                            f"Archivematica package has executed jobs ({len(executed_jobs)}) - waiting for the continuation of the processing"
                        )
                    else:
                        logging.info(
                            "No executed jobs for the given Archivematica package."
                        )
                except requests.HTTPError as e:
                    logging.info(
                        f"Error {e.response.status_code} for archivematica retreiving jobs"
                    )

                if is_failed and step.status == Status.WAITING:
                    # As long as the package is in queue to upload get_unit_status returns nothing so the waiting limit is checked
                    # If step has been waiting for more than AM_WAITING_TIME_LIMIT (mins), delete task
                    time_passed = (timezone.now() - step.start_date).total_seconds()
                    logging.info(f"Waiting in queue, time passed: {time_passed}s")
                    if time_passed > 60 * AM_WAITING_TIME_LIMIT:
                        logging.info(
                            f"Status Waiting limit reached ({AM_WAITING_TIME_LIMIT} mins) - deleting task"
                        )
                    else:
                        is_failed = False
                        am_status = {
                            "status": "WAITING",
                            "microservice": "Waiting for archivematica to respond",
                        }

                # If step status is not waiting, then archivematica delayed to respond so package creation is considered failed.
                # This is usually because archivematica may not have access to the file or the transfer source is not correct.
                if is_failed:
                    am_status = {
                        "status": "FAILED",
                        "microservice": "Archivematica delayed to respond.",
                    }
            else:
                # If there is other type of error code then archivematica connection could not be established.
                am_status = {
                    "status": "FAILED",
                    "microservice": "Error: Could not connect to archivematica",
                }
        except Exception as e:
            """
            In any other case make task fail (Archivematica crashed or not responding)
            """
            am_status = {"status": "FAILED", "microservice": str(e)}

        status = am_status["status"]
        microservice = am_status["microservice"]

        logger.info(f"Status for {step_id} is: {status}")

        # Needs to validate both because just status=complete does not guarantee that aip is stored
        if status == "COMPLETE" and microservice == "Remove the processing directory":
            am_status, step_status = _handle_completed_am_package(
                self, am, step, am_status, task_name, archive_id
            )
            step.set_status(step_status)

        elif status == "FAILED":
            step.set_status(Status.FAILED)
            _remove_periodic_task_on_failure(periodic_task, step)

        elif status == "PROCESSING" or status == "COMPLETE":
            step.set_status(Status.IN_PROGRESS)

        step.set_output_data(am_status)

    except Exception as e:
        logger.warning(
            f"Error while archiving {step.id}. Archivematica pipeline is full or settings configuration is wrong."
        )
        logger.warning(e)
        step.set_status(Status.FAILED)


def _get_am_client():
    # Get the current configuration
    am = AMClient()
    am.am_url = AM_URL
    am.am_user_name = AM_USERNAME
    am.am_api_key = AM_API_KEY
    am.transfer_source = AM_TRANSFER_SOURCE
    am.ss_url = AM_SS_URL
    am.ss_user_name = AM_SS_USERNAME
    am.ss_api_key = AM_SS_API_KEY
    am.processing_config = "automated"

    return am


def _handle_completed_am_package(self, am, step, am_status, task_name, archive_id):
    """
    Archivematica returns the uuid of the package, with this the storage service can be queried to get the AIP location.
    """

    uuid = am_status["uuid"]
    am.package_uuid = uuid
    aip = am.get_package_details()
    if type(aip) is dict:
        aip_path = aip["current_path"]
        aip_uuid = aip["uuid"]
        am_status["aip_uuid"] = aip_uuid
        am_status["aip_path"] = aip_path

        am_status["artifact"] = create_path_artifact(
            "AIP", os.path.join(AIP_UPSTREAM_BASEPATH, aip_path), aip_path
        )

        finalize(
            self=self,
            status=states.SUCCESS,
            retval={"status": 0},
            task_id=None,
            args=[archive_id, step.id],
            kwargs=None,
            einfo=None,
        )

        step.set_finish_date()
        step_status = Status.COMPLETED

        periodic_task = PeriodicTask.objects.get(name=task_name)
        periodic_task.delete()
    else:
        logger.error(f"AIP package with UUID {uuid} not found on {AM_SS_URL}")
        # If the path artifact is not complete try again
        step_status = Status.IN_PROGRESS

    return am_status, step_status


def _remove_periodic_task_on_failure(periodic_task, step):
    """
    Set step as failed and remove the scheduled task
    """
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
        access = "restricted"
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


def announce_sip(announce_path, creator, return_archive=False):
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
        if return_archive:
            return {"status": 0, "archive": archive}
        else:
            return {"status": 0, "archive_id": archive.id}

    else:
        return {"status": 1, "errormsg": "The given path is not a valid SIP"}


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


@shared_task(name="batch_announce_task", bind=True, ignore_result=True)
def batch_announce_task(self, announce_path, tag_id, user_id):
    # Run the "announce" procedure for every subfolder(validate, create an Archive, copy)
    user = User.objects.get(pk=user_id)
    tag = Collection.objects.get(pk=tag_id)

    for f in os.scandir(announce_path):
        try:
            if f.is_dir() and f.path != announce_path:
                announce_response = announce_sip(f.path, user, True)
                if announce_response["status"] == 0:
                    tag.add_archive(announce_response["archive"])
                else:
                    _add_error_to_tag_description(
                        tag, f.path, announce_response["errormsg"]
                    )
        except Exception as e:
            _add_error_to_tag_description(tag, f.path, f"Exception {str(e)}")

    tag.set_description(tag.description.replace("Batch Announce processing...", ""))
    if len(tag.description) == 0:
        tag.set_description("Batch Announce completed successfully")


def _add_error_to_tag_description(tag, path, errormsg):
    start_ind = tag.description.find(errormsg)
    if start_ind != -1:
        end_ind = start_ind + len(errormsg) + 1
        tag.set_description(
            tag.description[:end_ind] + path + "," + tag.description[end_ind:]
        )
    else:
        if tag.description.find("ERRORS:") == -1:
            tag.set_description(tag.description + " ERRORS:")
        tag.set_description(tag.description + f" {errormsg}:{path}.")


@shared_task(name="extract_title", bind=True, ignore_result=True, after_return=finalize)
def extract_title(self, archive_id, step_id):
    # For archives without title try to extract it from the metadata
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    sip_folder_name = archive.path_to_sip
    dublin_core_path = "data/meta/dc.xml"
    dublin_core_location = os.path.join(sip_folder_name, dublin_core_path)
    try:
        logging.info(f"Starting extract title from dc.xml for Archive {archive.id}")
        xml_tree = ET.parse(dublin_core_location)
        xml = xml_tree.getroot()
        ns = {
            "dc": "http://purl.org/dc/elements/1.1/",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        }
        title = xml.findall("./dc:dc/dc:title", ns)
        title = title[0].text
        logging.info(f"Title found for Archive {archive.id}: {title}")
        archive.set_title(title)
        return {"status": 0, "errormsg": None}
    except Exception as e:
        logging.warning(
            f"Error while extracting title from dc.xml at {dublin_core_location}: {str(e)}"
        )
        return {
            "status": 1,
            "errormsg": f"Title could not be extracted from Dublin Core file at {dublin_core_location}",
        }
