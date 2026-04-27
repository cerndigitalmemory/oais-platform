import errno
import os
import re
from datetime import timedelta
from pathlib import Path

import gfal2
import requests
from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.apps import apps
from django.db import models, transaction
from django.utils import timezone
from oais_utils.validate import compute_hash
from requests.exceptions import RetryError

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.models import Archive, Status, Step, StepName, StepType
from oais_platform.oais.tasks.pipeline_actions import create_retry_step, finalize
from oais_platform.oais.tasks.utils import (
    get_failure_type_from_status_code,
    set_and_return_error,
)
from oais_platform.settings import (
    AIP_UPSTREAM_BASEPATH,
    CTA_BASE_PATH,
    FTS_MAX_RETRY_COUNT,
    FTS_SOURCE_BASE_PATH,
    FTS_STATUS_INSTANCE,
)

logger = get_task_logger(__name__)


@shared_task(name="cta_manager", bind=True, ignore_result=True)
def cta_manager(self):
    """
    Manage the tasks needed for pushing archives to CTA:
     - check the number of transfers currently in progress
     - create tasks to check the statuses of finished FTS jobs
     - create tasks to submit new jobs
    """
    logger.info("Running CTA manager...")
    step_type = StepType.objects.get(name=StepName.PUSH_TO_CTA)

    try:
        current_transfers_count = _check_in_progress_jobs(self)
    except Exception as e:
        logger.error(f"Failed to check ongoing FTS transfers: {e}")
        return

    if current_transfers_count >= step_type.concurrency_limit:
        logger.info("Maximum number of transfers currently in progress.")
        return

    if step_type.enabled:
        amount = step_type.concurrency_limit - current_transfers_count
        _trigger_new_transfers(amount)
    else:
        logger.info(
            "Push to CTA step type is disabled. No new transfers will be triggered."
        )


@shared_task(name="push_to_cta", bind=True, ignore_result=True)
def push_to_cta(self, archive_id, step_id):
    """
    Push the AIP of the given Archive to CTA, preparing the FTS Job,
    locations etc, then saving the details of the operation as the output
    artifact.
    """
    logger.info(f"Pushing Archive {archive_id} to CTA")
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    step.set_start_date()

    if step.status != Status.WAITING:
        logger.warning(
            f"Step {step.id} is in status {step.status}, not triggering a transfer"
        )
        return

    if not archive.path_to_aip:
        set_and_return_error(
            step,
            {"status": 1, "errormsg": "AIP path not found for the given archive."},
            failure_type=StepFailureType.PATH_NOT_FOUND,
        )
        return

    cta_file_path = _get_cta_path(archive)

    try:
        if _verify_file(archive.path_to_aip, cta_file_path):
            msg = "Archive already exists on tape with the same size and checksum"
            _handle_successful_fts_job(
                self, step_id, archive_id, None, cta_file_path, msg
            )
            return
        overwrite = True

    except Exception as e:
        logger.warning(f"Error while verifing file on tape: {e}")
        overwrite = False

    try:
        fts = apps.get_app_config("oais").get_fts_client()
        submitted_job = fts.push_to_cta(
            f"{FTS_SOURCE_BASE_PATH}/{archive.path_to_aip}",
            f"{CTA_BASE_PATH}{cta_file_path}",
            overwrite,
        )
        with transaction.atomic():
            step.set_status(Status.IN_PROGRESS)
            step.set_task(self.request.id)
            step.set_output_data(
                {
                    "status": 0,
                    "artifact": {
                        "artifact_name": "FTS Job",
                        "artifact_path": cta_file_path,
                        "artifact_url": f"{FTS_STATUS_INSTANCE}/fts3/ftsmon/#/job/{submitted_job}",
                    },
                    "fts_job_id": submitted_job,
                }
            )

    except Exception as e:
        error = {"errormsg": str(e)}
        error["retry_count"] = _get_retry_count(step)
        failure_type = None
        if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
            failure_type = get_failure_type_from_status_code(e.response.status_code)
        elif isinstance(e, (ConnectionResetError, ConnectionError, RetryError)):
            failure_type = StepFailureType.CONNECTION_ERROR
        error["retrying"] = _retry_push_to_cta(step.archive.id, error["retry_count"])
        set_and_return_error(step, error, failure_type=failure_type)


@shared_task(name="fts_delegate", bind=True, ignore_result=True)
def fts_delegate(self):
    try:
        fts = apps.get_app_config("oais").get_fts_client()
        fts.check_ttl()
        fts.delegate()
    except Exception as e:
        logger.error(e)


def _get_cta_path(archive):
    try:
        return os.path.join(
            "aips", Path(archive.path_to_aip).relative_to(AIP_UPSTREAM_BASEPATH)
        )
    except ValueError:
        logger.warning(f"Unusual AIP path {archive.path_to_aip}")
        return os.path.join("aips", os.path.basename(archive.path_to_aip))


def _check_in_progress_jobs(self):
    in_progress_steps = Step.objects.filter(
        step_name=StepName.PUSH_TO_CTA, status=Status.IN_PROGRESS
    ).all()
    if not in_progress_steps:
        return 0

    steps_by_job_id = {}
    for step in in_progress_steps:
        job_id = step.output_data_json.get("fts_job_id")
        if job_id:
            steps_by_job_id[job_id] = step
        else:
            set_and_return_error(
                step,
                "Step has no fts_job_id",
                failure_type=StepFailureType.MISSING_OUTPUT_DATA,
            )

    logger.info("Checking statuses of ongoing transfers...")
    fts = apps.get_app_config("oais").get_fts_client()
    try:
        current_jobs = fts.job_statuses(list(steps_by_job_id.keys()))
    except Exception as e:
        _handle_jobs_not_found(e, steps_by_job_id)
        return _check_in_progress_jobs(self)
    finished_job_count = 0
    failed_job_count = 0

    for job in current_jobs:
        step = steps_by_job_id.get(job["job_id"])

        if job["job_state"] == "FINISHED":
            cta_file_path = _get_cta_path(step.archive)
            _handle_successful_fts_job(
                self, step.id, step.archive.id, job["job_id"], cta_file_path
            )
            finished_job_count += 1

        elif job["job_state"] == "FAILED":
            _handle_failed_fts_job(step, job)
            failed_job_count += 1

    if finished_job_count or failed_job_count:
        logger.info(
            f"Handled {finished_job_count} successful and {failed_job_count} failed FTS transfers."
        )
    else:
        logger.info("All transfers are in progress.")

    return len(current_jobs) - finished_job_count - failed_job_count


def _trigger_new_transfers(amount):
    # Fetch waiting push_to_cta steps for which the archive doesn't have previous steps in the pipeline
    waiting_steps = Step.objects.filter(
        step_name=StepName.PUSH_TO_CTA,
        status=Status.WAITING,
        archive__last_step_id=models.F("pk"),
    ).order_by("create_date")[:amount]

    if not waiting_steps:
        logger.info("No new transfers: no valid waiting push to CTA steps found.")
        return

    logger.info(f"Creating tasks to submit {len(waiting_steps)} new transfers.")
    for step in waiting_steps:
        push_to_cta.delay(step.archive.id, step.id)


def _handle_successful_fts_job(
    self, step_id, archive_id, job_id, cta_file_path, msg=None
):
    cta_artifact = {
        "artifact_name": "CTA",
        "artifact_localpath": cta_file_path,
        "artifact_url": f"{CTA_BASE_PATH}{cta_file_path}",
        "fts_id": job_id,
    }

    status = {"status": 0, "errormsg": None, "artifact": cta_artifact}
    if msg:
        status.update({"details": msg})

    finalize(
        self=self,
        current_status=states.SUCCESS,
        retval=status,
        task_id=None,
        args=[archive_id, step_id, None],
        kwargs=None,
        einfo=None,
    )


def _handle_failed_fts_job(step, status):
    result = {"FTS status": status}

    if step.output_data_json.get("artifact"):
        result["artifact"] = step.output_data_json["artifact"]

    result["retry_count"] = _get_retry_count(step)
    result["retrying"] = _retry_push_to_cta(step.archive.id, result["retry_count"])

    set_and_return_error(step, result)


def _get_retry_count(step):
    if step.input_step and step.input_step.step_type.name == StepName.PUSH_TO_CTA:
        return step.input_data_json.get("retry_count", -1) + 1
    return 0


def _retry_push_to_cta(archive_id, retry_count):
    if retry_count < FTS_MAX_RETRY_COUNT:
        logger.info(
            f"Retrying pushing archive {archive_id} to CTA (attempt {retry_count + 1})"
        )
        create_retry_step.apply_async(
            args=(archive_id, None, True, StepName.PUSH_TO_CTA),
            eta=timezone.now() + timedelta(hours=1),
        )
        return True
    logger.info(
        f"Quitting retrying pushing archive {archive_id} to CTA after {retry_count} attempts"
    )
    return False


def _handle_jobs_not_found(error, steps_by_job_id):
    not_found_ids = re.findall(
        r'No job with the id "([^"]+)" has been found', str(error)
    )
    if not not_found_ids:
        raise error
    logger.warning(
        f"{len(not_found_ids)} FTS jobs were not found and will be marked as failed."
    )
    for job_id in not_found_ids:
        if step := steps_by_job_id.get(job_id):
            _handle_failed_fts_job(
                step,
                {
                    "job_id": job_id,
                    "errormsg": f"FTS job {job_id} was not found. The job may have expired.",
                },
            )


def _verify_file(aip_path, cta_filename):
    try:
        gfal2.set_verbose(gfal2.verbose_level.warning)
        ctx = gfal2.creat_context()
        cta_path = f"{CTA_BASE_PATH}{cta_filename}"
        cta_size = ctx.stat(cta_path).st_size
        aip_size = Path(aip_path).stat().st_size

        if cta_size == aip_size:
            cta_file_checksum = ctx.checksum(cta_path, "ADLER32")
            aip_checksum = compute_hash(aip_path, alg="adler32")
            if cta_file_checksum == aip_checksum:
                logger.info("File already exists on tape. No transfer will be issued.")
                return True
            logger.info(
                "File exists but checksum differs. Initiating transfer to overwrite existing file..."
            )
        else:
            logger.info(
                "File exists but size differs. Initiating transfer to overwrite existing file..."
            )
        return False
    except gfal2.GError as e:
        if e.code == errno.ENOENT:  # no entry found
            logger.info("File not found on tape. Initiating transfer...")
            return False
        raise e
