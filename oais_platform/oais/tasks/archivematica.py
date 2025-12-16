import json
import ntpath
import os

import requests
from amclient import AMClient
from amclient.errors import error_codes, error_lookup
from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from oais_platform.oais.models import Archive, Status, Step, StepName, StepType
from oais_platform.oais.tasks.pipeline_actions import create_retry_step, finalize
from oais_platform.oais.tasks.utils import (
    create_path_artifact,
    remove_periodic_task_on_failure,
    set_and_return_error,
)
from oais_platform.settings import (
    AIP_UPSTREAM_BASEPATH,
    AM_API_KEY,
    AM_CALLBACK_DELAY,
    AM_POLLING_INTERVAL,
    AM_RETRY_LIMIT,
    AM_SS_API_KEY,
    AM_SS_URL,
    AM_SS_USERNAME,
    AM_TRANSFER_SOURCE,
    AM_URL,
    AM_USERNAME,
    AM_WAITING_TIME_LIMIT,
)

logger = get_task_logger(__name__)


@shared_task(
    name="archivematica",
    bind=True,
    ignore_result=True,
)
def archivematica(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Submit the SIP of the passed Archive to Archivematica
    preparing the call to the Archivematica API
    Once done, spawn a periodic task to check on the progress
    """
    current_step = Step.objects.get(pk=step_id)
    archive = Archive.objects.get(pk=archive_id)
    if (res := resource_check(self, current_step, archive)) != 0:
        return res

    path_to_sip = archive.path_to_sip

    logger.info(f"Starting archiving {path_to_sip}")

    sip_directory = ntpath.basename(path_to_sip)
    # Path to SIP inside Archivematica transfer source directory
    archivematica_dst = os.path.join(
        "/",
        sip_directory,
    )

    # Set up the AMClient to interact with the AM configuration provided in the settings
    am = get_am_client()
    am.transfer_directory = archivematica_dst
    am.transfer_name = get_transfer_name(archive)

    # Create archivematica package
    logger.info(
        f"Creating archivematica package on Archivematica instance: {AM_URL} at directory {archivematica_dst} for user {AM_USERNAME}"
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
                    "errormsg": None,
                }
            )

            create_check_am_status(package, current_step, archive_id, api_key)
            return current_step.output_data
    except requests.HTTPError as e:
        return set_and_return_error(
            current_step,
            f"Error while archiving {current_step.id}: status code {e.request.status_code}.",
            extra_log=f"HTTPError: {e}",
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
def check_am_status(self, message, step_id, archive_id, api_key=None):
    """
    Check the status of an Archivematica job by polling its API.
    The related Step is updated with the information returned from Archivematica
    e.g. the current microservice running or the final result.
    """
    step = Step.objects.get(pk=step_id)
    task_name = get_task_name(step)

    am = get_am_client()

    try:
        am_status = am.get_unit_status(message["id"])
        logger.info(f"Current unit status for {am_status}")
    except requests.HTTPError as e:
        logger.info(f"Error {e.response.status_code} for archivematica")
        if e.response.status_code == 400:
            is_failed = True
            try:
                # It is possible that the package is in queue between transfer and ingest - in this case it returns 400 but there are executed jobs
                executed_jobs = get_executed_jobs(am, message["id"])
                if executed_jobs > 0:
                    is_failed = False
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

            if is_failed and step.status == Status.WAITING:
                # As long as the package is in queue to upload get_unit_status returns nothing so the waiting limit is checked
                # If step has been waiting for more than AM_WAITING_TIME_LIMIT (mins), delete task
                time_passed = (timezone.now() - step.start_date).total_seconds()
                logger.info(f"Waiting in AM queue, time passed: {time_passed}s")
                if time_passed > 60 * AM_WAITING_TIME_LIMIT:
                    logger.info(
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
                    "errormsg": "Archivematica delayed to respond.",
                }
        else:
            # If there is other type of error code then archivematica connection could not be established.
            am_status = {
                "status": "FAILED",
                "errormsg": "Error: Could not connect to archivematica",
            }
    except Exception as e:
        """
        In any other case make task fail (Archivematica crashed or not responding)
        """
        am_status = {"status": "FAILED", "errormsg": str(e)}

    status = am_status["status"]
    microservice = am_status.get("microservice", None)

    logger.info(f"Status for {step_id} is: {status}")

    # Needs to validate both because just status=complete does not guarantee that aip is stored
    if status == "COMPLETE" and microservice == "Remove the processing directory":
        try:
            result = handle_completed_am_package(
                self, task_name, am, step, am_status, archive_id, api_key
            )
            if result:
                errors = get_executed_jobs(am, am_status["uuid"], check_for_failed=True)
                if errors and len(errors) > 0:
                    am_status["errormsg"] = errors
                    am_status["retry"] = True
                    logger.warning(
                        f"Archivematica reported {len(errors)} failed jobs for step {step.id}."
                    )
                    remove_periodic_task_on_failure(task_name, step, am_status)
                    step.set_status(Status.COMPLETED_WITH_WARNINGS)
        except Exception as e:
            logger.warning(
                f"Error while archiving {step.id}. Archivematica error while querying AIP details: {str(e)}"
            )
            remove_periodic_task_on_failure(
                task_name, step, {"status": "FAILED", "errormsg": str(e)}
            )

    elif status == "FAILED" or status == "REJECTED":
        if not am_status.get("errormsg", None):
            errors = get_executed_jobs(am, message["id"], check_for_failed=True)
            if am_status.get("uuid", None):
                errors += get_executed_jobs(
                    am, am_status["uuid"], check_for_failed=True
                )
            logger.warning(
                f"Archivematica reported {len(errors)} failed jobs for step {step.id}."
            )
            am_status["errormsg"] = errors
            am_status["retry"] = True
        remove_periodic_task_on_failure(task_name, step, am_status)

    elif status == "USER_INPUT":
        # this should not be possible with the automated pipeline but it happens sometimes
        logger.error(
            f"Package requires user input for step {step.id} - automatic pipeline failed"
        )
        am_status["errormsg"] = "Error: Archivematica requires user input."
        am_status["retry"] = True
        remove_periodic_task_on_failure(task_name, step, am_status)

    elif status == "PROCESSING" or status == "COMPLETE":
        step.set_output_data(am_status)
        step.set_status(Status.IN_PROGRESS)
        try:
            task = PeriodicTask.objects.get(name=task_name)
            task.enabled = True  # If it was triggered by a callback but not completed, re-enable it
            task.save()
        except PeriodicTask.DoesNotExist:
            logger.warning(f"PeriodicTask {task_name} for step {step.id} not found.")
    else:
        logger.warning(
            f"Unknown status from Archivematica: {status}, for step {step.id}"
        )
        step.set_output_data(am_status)

    if am_status.get("retry", False):
        retry_count = 0
        if step.input_step and step.input_step.step_type.name == StepName.ARCHIVE:
            input_data = json.loads(step.input_data) if step.input_data else {}
            retry_count = input_data.get("retry_count", 0)
        if retry_count + 1 > AM_RETRY_LIMIT:
            logger.warning("Max retries exceeded for failed Archivematica jobs.")
            am_status["retry_count"] = retry_count
            am_status["retry_limit_exceeded"] = True
            am_status["retry"] = False
        else:
            am_status["retry_count"] = retry_count + 1
            am_status["retry"] = True
            logger.info(f"Creating Archivematica retry step for archive {archive_id}")
            create_retry_step.apply_async(
                args=[
                    archive_id,
                    step.initiated_by_user.id if step.initiated_by_user else None,
                    True,
                    StepName.ARCHIVE,
                    api_key,
                ],
            )
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
                "incremented": False,
            },
        )
    with transaction.atomic():
        locked_archive_step_type = StepType.objects.select_for_update().get(
            pk=archive_step_type.id
        )
        if (
            locked_archive_step_type.current_count + 1
            > locked_archive_step_type.concurrency_limit
        ):
            exc_message = "Archivematica concurrency limit reached."
        elif (
            locked_archive_step_type.current_size_bytes + archive.sip_size
            > locked_archive_step_type.size_limit_bytes
        ):
            exc_message = "Archivematica aggregated file size limit reached."
        else:
            current_step.set_status(Status.WAITING)
            current_step.set_task(task.request.id)
            locked_archive_step_type.increment_current_count()
            locked_archive_step_type.increment_current_size(archive.sip_size)
            return 0

        logger.warning(exc_message)
        current_step.set_status(Status.WAITING)
        current_step.set_start_date(
            reset=True
        )  # reset start date to be picked up again
        current_step.set_output_data(
            {
                "status": 0,
                "message": "Archivematica is busy, waiting to start processing",
            }
        )
        return 1


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
                    errors.append(
                        {
                            "task": job["name"],
                            "microservice": job.get("microservice", None),
                            "link": f"{am.am_url}/tasks/{job['uuid']}",
                        }
                    )
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


def create_check_am_status(package, step, archive_id, api_key):
    # overwrite the start date so the waiting limit is counted from here
    step.set_start_date()
    task_name = get_task_name(step)
    # Check for the existing task by name
    if PeriodicTask.objects.filter(name=task_name).exists():
        raise Exception(
            f"Task '{task_name}' already exists, previous job is still in progress"
        )
    # Create the scheduler
    schedule, _ = IntervalSchedule.objects.get_or_create(
        every=AM_POLLING_INTERVAL, period=IntervalSchedule.MINUTES
    )
    # Spawn a periodic task to check for the status of the package on AM
    PeriodicTask.objects.create(
        interval=schedule,
        name=task_name,
        task="check_am_status",
        args=json.dumps([package, step.id, archive_id, api_key]),
        expire_seconds=AM_POLLING_INTERVAL * 60.0,
        last_run_at=timezone.now(),  # Otherwise tasks are sometimes not picked up
    )


def get_transfer_name(archive):
    # Adds an _ between Archive and the id because archivematica messes up with spaces
    transfer_name = (
        archive.source + "__" + archive.recid + "_Archive_" + str(archive.id)
    )
    if len(transfer_name) > 50:  # AM has a limit of 50 chars for transfer names
        transfer_name = "Archive_" + str(archive.id)

    return transfer_name


def get_task_name(step):
    transfer_name = get_transfer_name(step.archive)
    return f"AM Status for step: {step.id}, package: {transfer_name}"


@shared_task(
    name="callback_package",
    bind=True,
    ignore_result=True,
)
def callback_package(self, package_name):
    logger.info(f"Callback for package {package_name} received.")
    periodic_task = PeriodicTask.objects.filter(
        Q(name__endswith=package_name) | Q(name__regex=rf"^{package_name}_[0-9]+$")
    )  # Archivematica may append a suffix to the package name
    if periodic_task.count() > 1:
        logger.error(
            f"Ambiguous package name ({package_name}) found: {periodic_task.count()}"
        )
        return
    elif not periodic_task.exists():
        logger.error(f"Package with name {package_name} not found")
        return

    periodic_task = periodic_task.get()
    periodic_task.enabled = False
    periodic_task.save()

    args = json.loads(periodic_task.args)
    # Callback is triggered by post-store AIP but it's not the last step, need to start with a delay
    check_am_status.apply_async(args=args, countdown=AM_CALLBACK_DELAY)


def handle_completed_am_package(
    self, task_name, am, step, am_status, archive_id, api_key
):
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
        step.archive.set_aip_path(am_status["artifact"]["artifact_path"])
        step.archive.save()

        finalize(
            self=self,
            current_status=states.SUCCESS,
            retval={"status": 0},
            task_id=None,
            args=[archive_id, step.id, None, api_key],
            kwargs=None,
            einfo=None,
        )

        try:
            periodic_task = PeriodicTask.objects.get(name=task_name)
            periodic_task.delete()
        except PeriodicTask.DoesNotExist as e:
            logger.warning(e)
        except Exception as e:
            logger.error(e)
        return True
    else:
        retry_limit = 5
        output_data = {}
        if step.output_data:
            output_data = json.loads(step.output_data)
        retry_count = output_data.get("package_retry", 0)
        if retry_count + 1 > retry_limit:
            error_msg = f"AIP package with UUID {uuid} not found on {AM_SS_URL} after retrying {retry_limit} times."
            logger.error(error_msg)
            raise Exception(error_msg)
        else:
            logger.warning(
                f"AIP package with UUID {uuid} not found on {AM_SS_URL}, retrying..."
            )
            am_status["package_retry"] = retry_count + 1
            step.set_status(Status.IN_PROGRESS)
            step.set_output_data(am_status)
            return False
