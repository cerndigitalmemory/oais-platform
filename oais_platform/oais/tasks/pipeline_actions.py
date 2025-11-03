import json
import logging
import os

from celery import shared_task
from celery import states as celery_states
from celery.utils.log import get_task_logger
from django.contrib.auth.models import User
from django.db import transaction

from oais_platform.celery import app
from oais_platform.oais.models import Archive, Status, Step, StepName, StepType
from oais_platform.oais.tasks.utils import create_step

logger = get_task_logger(__name__)


def dispatch_task(
    step_type,
    archive_id,
    step_id,
    input_data=None,
    api_key=None,
    return_signature=False,
):
    sig = app.signature(
        step_type.task_name, args=(archive_id, step_id, input_data, api_key)
    )
    if return_signature:
        return sig
    return sig.delay()


def run_step(step, archive_id, api_key=None, return_signature=False):
    """
    Execute the given Step by spawning a Celery tasks for it

    step: target Step
    archive_id: ID of target Archive
    api_key: API key
    """
    # If no input_data, set the output of the input_step
    if step.input_data is None and step.input_step is not None:
        step.input_data = step.input_step.output_data

    # Set step execution start date
    step.set_start_date()

    # Set Archive's last_step to the current step
    with transaction.atomic():
        archive = Archive.objects.select_for_update().get(pk=archive_id)
        archive.set_last_step(step.id)

    if not step.step_type.enabled:
        step.set_status(Status.FAILED)
        step.set_output_data(
            {
                "status": 1,
                "errormsg": f"Step type {step.step_type.name} is disabled",
            }
        )
        archive.set_last_step(step.id)
        logging.warning(
            f"Step type {step.step_type.name} is disabled: setting step {step.id} to FAILED"
        )
        return step, None

    res = dispatch_task(
        step.step_type, archive_id, step.id, step.input_data, api_key, return_signature
    )

    return step, res


def execute_pipeline(
    archive_id, api_key=None, force_continue=False, return_signature=False
):

    with transaction.atomic():
        archive = Archive.objects.select_for_update().get(pk=archive_id)

        # Archive's pipeline is not running at the moment
        if archive.last_completed_step == archive.last_step or force_continue:
            # Run first available step in the pipeline
            if len(archive.pipeline_steps) != 0:
                step_id = archive.consume_pipeline()
                step = Step.objects.get(pk=step_id)
            # No available step in the pipeline
            else:
                # Automatically run next step ONLY if the automatic_next_step is set
                last_step = archive.last_completed_step

                next_step = last_step.step_type.automatic_next_step

                if next_step:
                    step = create_step(
                        step_name=next_step.name,
                        archive=archive,
                        input_step_id=last_step.id,
                        user=last_step.initiated_by_user,
                        harvest_batch=last_step.initiated_by_harvest_batch,
                    )
                else:
                    return None, None
        else:
            return None, None

    if step.status == Status.WAITING:
        return run_step(step, archive.id, api_key, return_signature)


@shared_task(name="create_retry_step", bind=True, ignore_result=True)
def create_retry_step(
    self, archive_id, user_id=None, execute=False, step_name=None, api_key=None
):
    archive = Archive.objects.get(pk=archive_id)
    last_step = Step.objects.get(pk=archive.last_step.id)
    if last_step and last_step.status != Status.FAILED:
        return {"errormsg": "Retry operation not permitted, last step is not failed."}
    if step_name and last_step.step_type.name != step_name:
        return {
            "errormsg": f"Retry operation not permitted, last step is not {step_name}."
        }
    step = create_step(
        step_name=last_step.step_type.name,
        archive=archive,
        input_step_id=last_step.id,
        input_data=last_step.output_data,
        user=User.objects.get(pk=user_id) if user_id else None,
        harvest_batch=last_step.initiated_by_harvest_batch,  # Keep tracking the batch to update batch status
    )

    # get steps that are preceded by the failed step
    next_steps = Step.objects.filter(input_step__id=last_step.id).exclude(id=step.id)

    # update successors of the failed steps
    for next_step in next_steps:
        next_step.set_input_step(step)
    archive.pipeline_steps.insert(0, step.id)
    archive.save()

    if execute:
        execute_pipeline(archive.id, api_key=api_key, force_continue=True)

    return {"errormsg": None}


def finalize(self, current_status, retval, task_id, args, kwargs, einfo):
    """
    This "callback" function is called everytime a Celery task
    finished its execution to update the status of the
    relevant Archive and Step.

    current_status: Celery task status
    retval: returned value from the execution of the celery task
    task_id: Celery task ID
    """
    # ID of the Archive this Step is in
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)

    # ID of the Step this task was spawned for
    step_id = args[1]
    step = Step.objects.get(pk=step_id)

    step.set_task(self.request.id)

    # If the Celery task succeded
    if current_status == celery_states.SUCCESS:
        # Even if the status is SUCCESS, the task may have failed
        # (e.g. without throwing an exception) so here we check
        # for returned errors
        if retval["status"] == 0:

            # Set step as completed and save finish date and output data
            step.set_status(Status.COMPLETED)
            step.set_finish_date()
            if step.step_type != StepType.get_by_stepname(StepName.ARCHIVE):
                step.set_output_data(retval)

            # If harvest, upload or announce is completed then add the audit of the sip.json to the
            #  archive.manifest field
            if step.step_type.has_sip:
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
                        logger.info("Sip.json audit saved at manifest field")
                except Exception:
                    logger.info(f"Sip.json was not found inside {sip_location}")

            # Set last_completed_step to the successful step
            with transaction.atomic():
                archive = Archive.objects.select_for_update().get(pk=archive_id)
                archive.set_last_completed_step(step_id)

            # Execute the remainig steps in the pipeline
            api_key = None
            if len(args) >= 4:
                api_key = args[3]
            execute_pipeline(archive_id, api_key=api_key)
        else:
            # Set the Step as failed and save the return value as the output data
            step.set_status(Status.FAILED)
            step.set_output_data(retval)
    else:
        step.set_status(Status.FAILED)
