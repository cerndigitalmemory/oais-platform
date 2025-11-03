import json
from datetime import timedelta

from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.apps import apps
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.pipeline_actions import create_retry_step, finalize
from oais_platform.oais.tasks.utils import remove_periodic_task_on_failure
from oais_platform.settings import (
    CTA_BASE_PATH,
    FTS_CONCURRENCY_LIMIT,
    FTS_MAX_RETRY_COUNT,
    FTS_SOURCE_BASE_PATH,
    FTS_STATUS_INSTANCE,
    FTS_WAIT_IN_HOURS,
    FTS_WAIT_LIMIT_IN_WEEKS,
)

logger = get_task_logger(__name__)


@shared_task(
    name="push_to_cta",
    bind=True,
    ignore_result=True,
    autoretry_for=(Exception,),
    max_retries=1,
    retry_kwargs={"countdown": FTS_WAIT_IN_HOURS * 60 * 60},
)
def push_to_cta(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Push the AIP of the given Archive to CTA, preparing the FTS Job,
    locations etc, then saving the details of the operation as the output
    artifact. Once done, set up another periodic task to check on
    the status of the transfer.
    """
    logger.info(f"Pushing Archive {archive_id} to CTA")

    task_name = f"Push to CTA: {step_id}"
    has_periodic_task = PeriodicTask.objects.filter(name=task_name).exists()

    # Get the Archive and Step we're running for
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    step.set_task(self.request.id)
    if not archive.path_to_aip:
        logger.warning("AIP path not found for the given archive.")
        step.set_status(Status.FAILED)
        step.set_output_data(
            {"status": 1, "errormsg": "AIP path not found for the given archive."}
        )
        return 1

    # Stop retrying after FTS_WAIT_LIMIT_IN_WEEKS
    if timezone.now() - step.start_date > timedelta(weeks=FTS_WAIT_LIMIT_IN_WEEKS):
        logger.info(f"Retry limit reached for step {step_id}, setting it to FAILED")
        remove_periodic_task_on_failure(
            task_name, step, {"status": 1, "errormsg": "Retry limit reached"}
        )
        return

    try:
        fts = apps.get_app_config("oais").fts

        # If already maximum number of transfers ongoing, create a periodic task for trying again
        if fts.number_of_transfers() >= FTS_CONCURRENCY_LIMIT:
            logger.info(
                f"Waiting for current transfers to finish before pushing archive {archive_id} to CTA"
            )
            if not has_periodic_task:
                schedule, _ = IntervalSchedule.objects.get_or_create(
                    every=FTS_WAIT_IN_HOURS, period=IntervalSchedule.HOURS
                )
                PeriodicTask.objects.get_or_create(
                    interval=schedule,
                    name=task_name,
                    task="push_to_cta",
                    args=json.dumps([archive_id, step_id, input_data, api_key]),
                    expire_seconds=FTS_WAIT_IN_HOURS * 60 * 60,
                )
            return

        # Remove periodic task if it exists
        if has_periodic_task:
            periodic_task = PeriodicTask.objects.get(name=task_name)
            periodic_task.delete()

        # And set the step as in progress
        step.set_status(Status.IN_PROGRESS)

        cta_folder_name = f"aip-{archive.id}"
        submitted_job = fts.push_to_cta(
            f"{FTS_SOURCE_BASE_PATH}/{archive.path_to_aip}",
            f"{CTA_BASE_PATH}{cta_folder_name}",
        )
    except Exception as e:
        if self.request.retries >= self.max_retries:
            logger.warning(str(e))
            step.set_status(Status.FAILED)
            step.set_output_data({"status": 1, "errormsg": str(e)})
            return 1

        logger.warning(f"Retrying pushing archive {archive_id} to CTA: {e}")
        raise e

    logger.info(submitted_job)

    output_cta_artifact = {
        "artifact_name": "FTS Job",
        "artifact_path": cta_folder_name,
        "artifact_url": f"{FTS_STATUS_INSTANCE}/fts3/ftsmon/#/job/{submitted_job}",
    }

    # Create the scheduler
    schedule, _ = IntervalSchedule.objects.get_or_create(
        every=1, period=IntervalSchedule.HOURS
    )
    # Spawn a periodic task to check for the status of the job
    PeriodicTask.objects.create(
        interval=schedule,
        name=f"FTS job status for step: {step.id}",
        task="check_fts_job_status",
        args=json.dumps([archive.id, step.id, submitted_job, api_key]),
        expire_seconds=3600.0,
    )

    step.set_output_data(
        {"status": 0, "artifact": output_cta_artifact, "fts_job_id": submitted_job}
    )


@shared_task(name="check_fts_job_status", bind=True, ignore_result=True)
def check_fts_job_status(self, archive_id, step_id, job_id, api_key=None):
    """
    Check the status of a FTS job.
    If finished, set the corresponding step as completed and remove the
    periodic task.
    """
    logger.info(f"Checking job status for Step {step_id} and job {job_id}")
    step = Step.objects.get(pk=step_id)
    task_name = f"FTS job status for step: {step.id}"

    try:
        fts = apps.get_app_config("oais").fts
        status = fts.job_status(job_id)
    except Exception as e:
        logger.warning(str(e))
        remove_periodic_task_on_failure(
            task_name, step, {"status": 1, "errormsg": str(e)}
        )

    logger.info(f"FTS job status for Step {step_id} returned: {status['job_state']}.")

    if status["job_state"] == "FINISHED":
        _handle_completed_fts_job(self, task_name, step, archive_id, job_id, api_key)
    elif status["job_state"] == "FAILED":
        result = {"FTS status": status}
        input_data = json.loads(step.input_data)
        output_data = json.loads(step.output_data)
        if output_data["artifact"]:
            result["artifact"] = output_data["artifact"]
        result["retry_count"] = input_data.get("retry_count", -1) + 1

        if result["retry_count"] < FTS_MAX_RETRY_COUNT:
            logger.info(
                f"Retrying pushing archive {archive_id} to CTA (attempt {result['retry_count'] + 1})"
            )
            result["retrying"] = True
            create_retry_step.apply_async(
                args=(archive_id, None, True, StepName.PUSH_TO_CTA, api_key),
                eta=timezone.now() + timedelta(hours=1),
            )
        else:
            logger.info(
                f"Quitting retrying pushing archive {archive_id} to CTA after {result['retry_count']} attempts"
            )
            result["retrying"] = False

        remove_periodic_task_on_failure(task_name, step, result)


@shared_task(name="fts_delegate", bind=True, ignore_result=True)
def fts_delegate(self):
    try:
        fts = apps.get_app_config("oais").fts
        fts.check_ttl()
        fts.delegate()
    except Exception as e:
        logger.error(e)


def _handle_completed_fts_job(self, task_name, step, archive_id, job_id, api_key=None):
    try:
        periodic_task = PeriodicTask.objects.get(name=task_name)
    except Exception as e:
        logger.warning(e)
        step.set_status(Status.FAILED)
        return

    logger.info("FTS transfer succeded, removing periodic task")
    periodic_task.delete()

    cta_folder_name = f"aip-{archive_id}"
    cta_artifact = {
        "artifact_name": "CTA",
        "artifact_localpath": cta_folder_name,
        "artifact_url": f"{CTA_BASE_PATH}{cta_folder_name}",
        "fts_id": job_id,
    }

    status = {"status": 0, "errormsg": None, "artifact": cta_artifact}
    finalize(
        self=self,
        current_status=states.SUCCESS,
        retval=status,
        task_id=None,
        args=[archive_id, step.id, None, api_key],
        kwargs=None,
        einfo=None,
    )
