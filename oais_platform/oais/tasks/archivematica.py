import os
from pathlib import Path

import requests
from amclient import AMClient
from amclient.errors import error_codes, error_lookup
from celery import chord, shared_task, states
from celery.utils.log import get_task_logger
from django.db import transaction
from django.utils import timezone

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.exceptions import MaxRetriesExceeded
from oais_platform.oais.models import (
    COMPLETED_STATUSES,
    Status,
    Step,
    StepName,
    StepType,
)
from oais_platform.oais.tasks.pipeline_actions import create_retry_step, finalize
from oais_platform.oais.tasks.utils import (
    create_path_artifact,
    get_failure_type_from_status_code,
    set_and_return_error,
)
from oais_platform.settings import (
    AIP_UPSTREAM_BASEPATH,
    AM_API_KEY,
    AM_PROCESSING_TIME_LIMIT,
    AM_RETRY_LIMIT,
    AM_SS_API_KEY,
    AM_SS_URL,
    AM_SS_USERNAME,
    AM_TRANSFER_SOURCE,
    AM_URL,
    AM_USERNAME,
    AM_WAITING_TIME_LIMIT,
    SIP_UPSTREAM_BASEPATH,
)

logger = get_task_logger(__name__)


@shared_task(
    name="archivematica",
    bind=True,
    ignore_result=True,
)
def archivematica(self, step_id):
    """
    Submit the SIP of the passed Archive to Archivematica
    preparing the call to the Archivematica API
    Once done, spawn a periodic task to check on the progress
    """
    current_step = Step.objects.get(pk=step_id)
    archive = current_step.archive
    if (res := resource_check(self, current_step, archive)) != 0:
        return res

    path_to_sip = archive.path_to_sip

    logger.info(f"Starting archiving {path_to_sip}")

    # Path to SIP inside Archivematica transfer source directory
    archivematica_dst = os.path.join(
        "/",
        Path(path_to_sip).relative_to(SIP_UPSTREAM_BASEPATH),
    )

    # Set up the AMClient to interact with the AM configuration provided in the settings
    am = get_am_client()
    am.transfer_directory = archivematica_dst
    am.transfer_name = get_transfer_name(archive, current_step)

    # Create archivematica package
    logger.info(
        f"Creating archivematica package on Archivematica instance: {AM_URL} at directory {archivematica_dst}"
    )

    try:
        package = am.create_package()
        if isinstance(package, (str, int)) and package in error_codes:
            """
            The AMClient will return error codes when there was an error in the request to the AM API.
            We can't do much in these cases, a part from suggesting to take a look at the AM logs.
            Check 'amclient/errors' for more information.
            """
            errormsg = error_lookup(package)
            return set_and_return_error(
                current_step,
                f"Error while archiving {current_step.id}. AM create returned error {package}: {errormsg}",
            )
        else:
            current_step.set_output_data(
                {
                    "status": 0,
                    "details": "Uploaded to Archivematica - waiting for processing",
                    "package_uuid": package["id"],
                    "transfer_name": am.transfer_name,
                    "errormsg": None,
                }
            )
            current_step.set_status(Status.SUBMITTED)
            current_step.set_task(self.request.id)
            return current_step.output_data_json
    except requests.HTTPError as e:
        return set_and_return_error(
            current_step,
            f"Error while archiving {current_step.id}: status code {e.request.status_code}.",
            extra_log=f"HTTPError: {e}",
            failure_type=get_failure_type_from_status_code(e.request.status_code),
        )
    except Exception as e:
        return set_and_return_error(
            current_step, f"Error while archiving {current_step.id}: {str(e)}"
        )


@shared_task(
    name="check_am_status",
    bind=True,
    ignore_result=True,
)
def check_am_status(self, step_id):
    """
    Check the status of an Archivematica job by polling its API.
    The related Step is updated with the information returned from Archivematica
    e.g. the current microservice running or the final result.
    """
    step = Step.objects.get(pk=step_id)

    am = get_am_client()
    uuid = step.output_data_json.get("package_uuid", None)

    try:
        failure_type = None
        am_status = None
        if uuid is None:
            failure_type = StepFailureType.MISSING_OUTPUT_DATA
            raise ValueError("No package UUID found in step output data.")
        else:
            am_status = am.get_unit_status(uuid)
        logger.info(f"Current unit status for {am_status}")
    except requests.HTTPError as e:
        logger.info(f"Error {e.response.status_code} for archivematica")
        failure_type = get_failure_type_from_status_code(e.response.status_code)
        if e.response.status_code == 400:
            try:
                # It is possible that the package is in queue between transfer and ingest - in this case it returns 400 but there are executed jobs
                executed_jobs = get_executed_jobs(am, uuid)
                if executed_jobs > 0:
                    am_status = {
                        "status": "PROCESSING",
                        "microservice": "Waiting for archivematica to continue the processing",
                    }
                    logger.info(
                        f"Archivematica package has executed jobs ({executed_jobs}) - waiting for the continuation of the processing"
                    )
                else:
                    logger.info("No executed jobs for the given Archivematica package.")
            except requests.HTTPError as e:
                logger.info(
                    f"Error {e.response.status_code} for archivematica retreiving jobs"
                )

            if not am_status:
                # As long as the package is in queue to upload get_unit_status returns nothing so the waiting limit is checked
                # If step has been waiting for more than AM_WAITING_TIME_LIMIT (mins), delete task
                time_passed = (timezone.now() - step.start_date).total_seconds()
                logger.info(f"Waiting in AM queue, time passed: {time_passed}s")
                if time_passed > 60 * AM_WAITING_TIME_LIMIT:
                    logger.info(
                        f"Status Waiting limit reached ({AM_WAITING_TIME_LIMIT} mins) - setting to failed for step {step.id}"
                    )
                    am_status = {
                        "status": "FAILED",
                        "errormsg": "Archivematica delayed to respond.",
                    }
                    failure_type = StepFailureType.TIMEOUT
                else:
                    am_status = {
                        "status": "WAITING",
                        "microservice": "Waiting for archivematica to respond",
                    }
        else:
            # If there is other type of error code then archivematica connection could not be established.
            am_status = {
                "status": "FAILED",
                "errormsg": "Error: Could not connect to archivematica",
            }
            failure_type = StepFailureType.CONNECTION_ERROR
    except Exception as e:
        """
        In any other case make task fail (Archivematica crashed or not responding)
        """
        am_status = {"status": "FAILED", "errormsg": str(e)}

    status = am_status["status"]
    microservice = am_status.get("microservice", None)
    am_status["transfer_name"] = step.output_data_json.get("transfer_name", None)
    am_status["package_uuid"] = uuid

    logger.info(f"Status for {step_id} is: {status}")

    # Needs to validate both because just status=complete does not guarantee that aip is stored
    if status == "COMPLETE" and microservice == "Remove the processing directory":
        try:
            handle_completed_am_package(self, am, step, am_status)
        except Exception as e:
            logger.warning(
                f"Error while archiving {step.id}. Archivematica error while querying AIP details: {str(e)}"
            )
            if isinstance(e, MaxRetriesExceeded):
                failure_type = StepFailureType.PACKAGE_NOT_FOUND
            set_and_return_error(
                step,
                {"status": "FAILED", "errormsg": str(e)},
                failure_type=failure_type,
            )

    elif status == "FAILED" or status == "REJECTED":
        if not am_status.get("errormsg", None):
            errors = get_executed_jobs(am, uuid, check_for_failed=True)
            if am_status.get("uuid", None):
                errors += get_executed_jobs(
                    am, am_status["uuid"], check_for_failed=True
                )
            logger.warning(
                f"Archivematica reported {len(errors)} failed jobs for step {step.id}."
            )
            am_status["errormsg"] = errors
            am_status["retry"] = True
        if failure_type == StepFailureType.TIMEOUT:
            am_status["retry"] = True
        set_and_return_error(step, am_status, failure_type=failure_type)

    elif status == "USER_INPUT":
        # this should not be possible with the automated pipeline but it happens sometimes
        logger.error(
            f"Package requires user input for step {step.id} - automatic pipeline failed"
        )
        am_status["errormsg"] = "Error: Archivematica requires user input."
        am_status["retry"] = True
        set_and_return_error(
            step, am_status, failure_type=StepFailureType.USER_INPUT_REQUIRED
        )

    elif status == "PROCESSING" or status == "COMPLETE":
        time_passed = (timezone.now() - step.start_date).total_seconds()
        if time_passed > 60 * AM_PROCESSING_TIME_LIMIT:  # Probably stuck in processing
            logger.info(
                f"Processing time limit reached ({AM_PROCESSING_TIME_LIMIT} mins) - setting step {step.id} to failed"
            )
            am_status["errormsg"] = (
                "Error: Archivematica processing time limit reached."
            )
            am_status["retry"] = True
            set_and_return_error(step, am_status, failure_type=StepFailureType.TIMEOUT)
        else:
            step.set_output_data(am_status)
            step.set_status(Status.IN_PROGRESS)
    elif status == "WAITING":
        step.set_status(Status.SUBMITTED)
        step.set_output_data(am_status)
    else:
        logger.warning(
            f"Unknown status from Archivematica: {status}, for step {step.id}"
        )
        step.set_output_data(am_status)

    if am_status.get("retry", False):
        retry_count = 0
        if step.input_step and step.input_step.step_type.name == StepName.ARCHIVE:
            retry_count = step.input_data_json.get("retry_count", 0)
        if retry_count + 1 > AM_RETRY_LIMIT:
            logger.warning("Max retries exceeded for failed Archivematica jobs.")
            am_status["retry_count"] = retry_count
            am_status["retry_limit_exceeded"] = True
            am_status["retry"] = False
        else:
            am_status["retry_count"] = retry_count + 1
            am_status["retry"] = True
            logger.info(
                f"Creating Archivematica retry step for archive {step.archive.id}"
            )
            create_retry_step.apply_async(
                args=(
                    step.archive.id,
                    step.initiated_by_user.id if step.initiated_by_user else None,
                    False,
                    StepName.ARCHIVE,
                )
            )
        step.set_finish_date()
        step.set_output_data(am_status)


def resource_check(task, current_step, archive):
    if archive.sip_size == 0:
        archive.update_sip_size()
    archive_step_type = StepType.get_by_stepname(StepName.ARCHIVE)
    if archive.sip_size > archive_step_type.size_limit_bytes:
        return set_and_return_error(
            current_step,
            {
                "status": 1,
                "errormsg": f"SIP exceeds the Archivematica file size limit ({archive_step_type.size_limit_bytes // (1024**3)}GB).",
            },
            failure_type=StepFailureType.SIZE_EXCEEDED,
        )
    with transaction.atomic():
        locked_archive_step_type = StepType.objects.select_for_update().get(
            pk=archive_step_type.id
        )
        if (
            locked_archive_step_type.current_size_bytes + archive.sip_size
            > locked_archive_step_type.size_limit_bytes
        ):
            logger.warning("Archivematica aggregated file size limit reached.")
            current_step.set_status(Status.WAITING)
            current_step.set_output_data(
                {
                    "status": 0,
                    "message": "Archivematica is busy, waiting to start processing",
                }
            )
            return 1
        else:
            return 0


def get_am_client():
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


def get_executed_jobs(am, unit_uuid, check_for_failed=False):
    am.unit_uuid = unit_uuid
    executed_jobs = am.get_jobs()
    logger.debug(f"Executed jobs for given id({unit_uuid}): {executed_jobs}")
    errors = []
    if executed_jobs != 1 and len(executed_jobs) > 0:
        if not check_for_failed:
            return len(executed_jobs)
        for job in executed_jobs:
            try:
                # Normalization failure is not failing the whole package, so need to check tasks inside the job
                if (
                    job["name"] == "Normalize for preservation"
                    and job["status"] == "COMPLETE"
                ):
                    for task in job["tasks"]:
                        if task["exit_code"] == 1:
                            task_uuid = task["uuid"]
                            filename = None
                            result = requests.get(
                                f"{am.am_url}/api/v2beta/task/{task_uuid}",
                                headers=am._am_auth_headers(),
                            )
                            if result.ok:
                                task_info = result.json()
                                filename = task_info.get("file_name", None)
                            errors.append(
                                {
                                    "task": job["name"],
                                    "filename": filename,
                                    "link": f"{am.am_url}/task/{task_uuid}",
                                }
                            )
                if job["status"] == "FAILED":
                    entry = {
                        "task": job["name"],
                        "microservice": job.get("microservice"),
                        "link": f"{am.am_url}/tasks/{job['uuid']}",
                    }
                    if not any(
                        e["task"] == entry["task"]
                        and e.get("microservice", None) == entry["microservice"]
                        and e["link"] == entry["link"]
                        for e in errors
                    ):
                        errors.append(entry)
            except KeyError:
                logger.warning(
                    f"KeyError while checking executed jobs for {unit_uuid}: {str(job)}"
                )
            except Exception as e:
                logger.warning(
                    f"Error while checking executed jobs for {unit_uuid}: {str(e)}"
                )
        return errors
    else:
        return 0


def get_transfer_name(archive, step):
    # Adds an _ between Archive and the id because archivematica messes up with spaces
    transfer_name = (
        archive.source
        + "_"
        + archive.recid
        + "_Archive_"
        + str(archive.id)
        + "_Step_"
        + str(step.id)
    )
    if len(transfer_name) > 50:  # AM has a limit of 50 chars for transfer names
        transfer_name = "Archive_" + str(archive.id) + "_Step_" + str(step.id)

    return transfer_name


def handle_completed_am_package(self, am, step, am_status):
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

        step.set_output_data(am_status)
        if step.archive.path_to_aip != am_status["artifact"]["artifact_path"]:
            step.archive.set_aip_path(am_status["artifact"]["artifact_path"])
            step.archive.save()

            outdate_aip_dependent_steps(step.archive)

        errors = get_executed_jobs(am, am_status["uuid"], check_for_failed=True)
        if errors and len(errors) > 0:
            am_status["errormsg"] = errors
            am_status["retry"] = True
            logger.warning(
                f"Archivematica reported {len(errors)} failed jobs for step {step.id}."
            )
            step.set_status(Status.COMPLETED_WITH_WARNINGS)
            step.set_output_data(am_status)
        else:
            finalize(
                self=self,
                current_status=states.SUCCESS,
                retval={"status": 0},
                task_id=None,
                args=[step.archive.id, step.id, None],
                kwargs=None,
                einfo=None,
            )
    else:
        retry_limit = 5
        retry_count = step.output_data_json.get("package_retry", 0)
        if retry_count + 1 > retry_limit:
            error_msg = f"AIP package with UUID {uuid} not found on {AM_SS_URL} after retrying {retry_limit} times."
            logger.error(error_msg)
            raise MaxRetriesExceeded(error_msg)
        else:
            logger.warning(
                f"AIP package with UUID {uuid} not found on {AM_SS_URL}, retrying..."
            )
            am_status["package_retry"] = retry_count + 1
            step.set_status(Status.IN_PROGRESS)
            step.set_output_data(am_status)


@shared_task(name="archive_failed_count_reset")
def archive_failed_count_reset():
    step_type = StepType.objects.get(name=StepName.ARCHIVE)
    if step_type.enabled and step_type.failed_count > 0:
        logger.info(f"Resetting failed count for step type {step_type.name}")
        step_type.failed_count = 0
        step_type.save()


def outdate_aip_dependent_steps(archive):
    """Outdate all steps that depend on the AIP."""
    steps = archive.steps.filter(
        step_type__name__in=[
            StepName.PUSH_TO_CTA,
            StepName.INVENIO_RDM_PUSH,
            StepName.NOTIFY_SOURCE,
        ],
        status__in=COMPLETED_STATUSES,
    )
    for step in steps:
        step.set_status(Status.OUTDATED)
        step.set_output_data_field("outdated_at", timezone.now().isoformat())
    if steps.count() > 0:
        logger.info(
            f"Outdated {steps.count()} steps that depend on AIP for Archive {archive.id}"
        )


@shared_task(
    name="am_manager",
    bind=True,
    ignore_result=True,
)
def am_manager(self):
    logger.info("Running Archivematica manager...")

    in_progress_steps = Step.objects.filter(
        step_type__name=StepName.ARCHIVE,
        status__in=[Status.IN_PROGRESS, Status.SUBMITTED],
    )
    count = in_progress_steps.count()
    logger.info(f"Current number of in progress Archivematica steps: {count}")

    if count == 0:
        start_am_transfers.apply_async()
        return

    logger.info(f"Checking status of {count} in progress Archivematica steps...")

    chord(check_am_status.s(step.id) for step in in_progress_steps)(
        start_am_transfers.s()
    )


@shared_task(
    name="start_am_transfers",
    bind=True,
    ignore_result=True,
)
def start_am_transfers(self, chord_results=None):
    logger.info("Starting Archivematica transfers...")
    step_type = StepType.objects.get(name=StepName.ARCHIVE)
    capacity = step_type.concurrency_limit - step_type.current_count

    if capacity <= 0:
        logger.info("Maximum number of Archivematica steps currently in progress.")
        return

    if not step_type.enabled:
        logger.info("Archivematica step type is currently disabled.")
        return
    logger.info(
        f"Checking for waiting Archivematica steps to run, capacity: {capacity}..."
    )
    waiting_steps = Step.objects.filter(
        step_type__name=StepName.ARCHIVE,
        status=Status.WAITING,
    ).order_by("create_date")[:capacity]

    if not waiting_steps.exists():
        logger.info("No waiting Archivematica steps to start")
        return

    logger.info(f"Starting {waiting_steps.count()} waiting Archivematica steps to run.")
    for step in waiting_steps:
        archivematica.apply_async(args=[step.id])
