import ast
import json
import logging
import ntpath
import os
import shutil
import time
import uuid
from datetime import datetime, timedelta
from logging import log

from amclient import AMClient
from bagit_create import main as bic
from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.settings import (
    AM_ABS_DIRECTORY,
    AM_API_KEY,
    AM_REL_DIRECTORY,
    AM_TRANSFER_SOURCE,
    AM_URL,
    AM_USERNAME,
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

            # Update the next possible steps
            next_steps = archive.update_next_steps(step.name)

            if len(next_steps) == 1:
                create_step(next_steps[0], archive_id, step_id)
        else:
            step.set_status(Status.FAILED)
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
        # change to waiting/not run
        status=Status.IN_PROGRESS,
    )

    archive = Step.objects.get(pk=archive_id)

    # Consider switching this to "eval"?
    if step_name == Steps.HARVEST:
        task = process.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.VALIDATION:
        task = validate.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.CHECKSUM:
        task = checksum.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.ARCHIVE:
        task = archivematica.delay(step.archive.id, step.id, step.input_data)


# Steps implementations
@shared_task(name="process", bind=True, ignore_result=True, after_return=finalize)
def process(self, archive_id, step_id):
    """
    Run BagIt-Create to harvest data from upstream, preparing a
    Submission Package (SIP)
    """
    logger.info(f"Starting harvest of archive {archive_id}")

    archive = Archive.objects.get(pk=archive_id)

    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    bagit_result = bic.process(
        recid=archive.recid,
        source=archive.source,
        loglevel=2,
    )

    return bagit_result


@shared_task(name="validate", bind=True, ignore_result=True, after_return=finalize)
def validate(self, archive_id, step_id, input_data):
    res = ast.literal_eval(input_data)

    path_to_sip = res["foldername"]
    logger.info(f"Starting SIP validation {path_to_sip}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    # Set task id
    current_step.set_task(self.request.id)

    # Checking registry = checking if the folder exists
    sip_exists = os.path.exists(path_to_sip)

    if not sip_exists:
        return {"status": 1}

    # Runs validate_sip from oais_utils
    valid = validate_sip(path_to_sip)

    return {"status": 0, "errormsg": None, "foldername": path_to_sip}


@shared_task(name="checksum", bind=True, ignore_result=True, after_return=finalize)
def checksum(self, archive_id, step_id, input_data):
    res = ast.literal_eval(input_data)

    path_to_sip = res["foldername"]
    logger.info(f"Starting checksum validation {path_to_sip}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    # Set task id
    current_step.set_task(self.request.id)

    sip_exists = os.path.exists(path_to_sip)
    if not sip_exists:
        return {"status": 1}

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

    tempfile = os.path.join(os.path.dirname(path_to_sip), str(uuid.uuid4()))
    with open(tempfile, "w") as f:
        json.dump(data, f, indent=4)

    # rename temporary file to sip2 json
    new_sip_json = os.path.join(path_to_sip, "data/meta/sip2.json")
    os.rename(tempfile, new_sip_json)

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
    res = ast.literal_eval(input_data)
    path_to_sip = res["foldername"]
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

    # Copy the folders and the contents to the archivematica transfer source folder
    try:
        shutil.copytree(path_to_sip, system_dst)
    except FileExistsError:
        logging.warning("File exists.")

    # Get configuration from archivematica from settings
    am = AMClient()
    am.am_url = AM_URL
    am.am_user_name = AM_USERNAME
    am.am_api_key = AM_API_KEY
    am.transfer_source = AM_TRANSFER_SOURCE
    am.transfer_directory = archivematica_dst
    am.transfer_name = ntpath.basename(path_to_sip) + "::Archive " + str(archive_id.id)
    am.processing_config = "automated"

    # Create archivematica package
    logging.info(
        f"Creating archivematica package on Archivematica instance: {AM_URL} at directory {archivematica_dst} for user {AM_USERNAME}"
    )

    try:
        # After 2 seconds check if the folder has been transfered to archivematica
        package = am.create_package()

        step = Step.objects.get(pk=step_id)
        step.set_status(Status.NOT_RUN)

        # Create the scheduler (sets every 10 seconds)
        schedule = IntervalSchedule.objects.create(
            every=5, period=IntervalSchedule.SECONDS
        )
        # Create a periodic task that checks the status of archivematica avery 10 seconds.
        PeriodicTask.objects.create(
            interval=schedule,
            name=f"Archivematica status for step: {current_step.id}",
            task="check_am_status",
            args=json.dumps([package, current_step.id, archive_id.id]),
            expires=timezone.now() + timedelta(minutes=600),
        )

    except Exception as e:
        logger.error(
            f"Error while archiving {current_step.id}. Check your archivematica settings configuration."
        )
        current_step.set_status(Status.FAILED)
        return {"status": 1, "message": e}

    return {"status": 0, "message": "Uploaded to Archivematica"}


@shared_task(
    name="check_am_status",
    bind=True,
    ignore_result=True,
)
def check_am_status(self, message, step_id, archive_id):

    step = Step.objects.get(pk=step_id)
    task_name = f"Archivematica status for step: {step_id}"

    # Get the current configuration
    am = AMClient()
    am.am_url = AM_URL
    am.am_user_name = AM_USERNAME
    am.am_api_key = AM_API_KEY
    am.transfer_source = AM_TRANSFER_SOURCE

    try:
        periodic_task = PeriodicTask.objects.get(name=task_name)

        try:
            am_status = am.get_unit_status(message["id"])
        except:
            if step.status == Status.NOT_RUN:
                pass

        status = am_status["status"]
        microservice = am_status["microservice"]
        logger.info(f"Status for {step_id} is: {status}")
        if status == "COMPLETE":
            step.set_finish_date()
            step.set_status(Status.COMPLETED)

            periodic_task = PeriodicTask.objects.get(name=task_name)
            periodic_task.delete()

            finalize(
                self=self,
                status=states.SUCCESS,
                retval={"status": 0},
                task_id=None,
                args=[archive_id, step_id],
                kwargs=None,
                einfo=None,
            )

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
        step.set_status(Status.FAILED)
