from bagit_create import main as bic
from celery import states
from celery.decorators import task
from celery.utils.log import get_task_logger
from oais_platform.oais.models import Archive, Step, Status, Steps
from django.utils import timezone
import ast

from oais_utils.validate import validate_sip

import json
import os
import uuid

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
    id = args[0]
    archive = Archive.objects.get(pk=id)

    # ID of the Step this task was spawned for
    id = args[1]
    step = Step.objects.get(pk=id)

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
            step.set_output_data(retval)

            # Update the next possible steps
            archive.update_next_steps(step.name)

            # Run next step
            # run_next_step(archive.id, step.id)

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
    # archive.set_step(step.id)

    # Consider switching this to "eval"?
    if step_name == Steps.HARVEST:
        task = process.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.VALIDATION:
        task = validate.delay(step.archive.id, step.id, step.input_data)
    elif step_name == Steps.CHECKSUM:
        task = checksum.delay(step.archive.id, step.id, step.input_data)

    # step.celery_task_id = task.id


# Steps implementations
@task(name="process", bind=True, ignore_result=True, after_return=finalize)
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


def validate_after_return(self, status, retval, task_id, args, kwargs, einfo):
    # id is the first parameter passed to the task
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)

    path_to_sip = args[1]
    # Could be failed registry_check/validation or successful validation
    step_id = args[2]
    step = Step.objects.get(pk=step_id)

    if status == states.SUCCESS:
        if retval:
            step.set_status(Status.COMPLETED)
            archive.set_step(step)

            # Next step
            next_step = Step.objects.create(
                archive=step.archive,
                name=Steps.CHECKSUM,
                input_step=step,
                input_data=step.output_data,
                status=Status.WAITING_APPROVAL,
            )

            archive = step.archive

            checksum.delay(next_step.id, path_to_sip)
        else:
            logger.error(f"Error while validating sip {id}")
            step.set_status(Status.FAILED)
    else:
        step.set_status(Status.FAILED)


@task(name="validate", bind=True, ignore_result=True, after_return=finalize)
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
        return False

    # Runs validate_sip from oais_utils
    valid = validate_sip(path_to_sip)

    return {"status": 0, "errormsg": None, "foldername": path_to_sip}


def checksum_after_return(self, status, retval, task_id, args, kwargs, einfo):

    path_to_sip = args[1]
    step_id = args[0]
    step = Step.objects.get(pk=step_id)

    if status == states.SUCCESS:
        if retval:
            step.set_status(Status.COMPLETED)

        else:
            logger.error(f"Error while checksuming {path_to_sip}")
            step.set_status(Status.FAILED)
    else:
        step.set_status(Status.FAILED)


@task(name="checksum", bind=True, ignore_result=True, after_return=finalize)
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
        return False

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
                    return False

    tempfile = os.path.join(os.path.dirname(path_to_sip), str(uuid.uuid4()))
    with open(tempfile, "w") as f:
        json.dump(data, f, indent=4)

    # rename temporary file to sip2 json
    new_sip_json = os.path.join(path_to_sip, "data/meta/sip2.json")
    os.rename(tempfile, new_sip_json)

    logger.info(f"Checksum completed!")

    return {"status": 0, "errormsg": None, "foldername": path_to_sip}
