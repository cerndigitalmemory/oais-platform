import json
import ntpath
import os

import requests
from amclient import AMClient
from amclient.errors import error_codes, error_lookup
from celery import shared_task, states
from celery.exceptions import Retry
from celery.utils.log import get_task_logger
from django.db import OperationalError, transaction
from django.db.models import Sum
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.pipeline_actions import finalize
from oais_platform.oais.tasks.utils import (
    create_path_artifact,
    remove_periodic_task_on_failure,
    set_and_return_error,
)
from oais_platform.settings import (
    AGGREGATED_FILE_SIZE_LIMIT,
    AIP_UPSTREAM_BASEPATH,
    AM_API_KEY,
    AM_CALLBACK_DELAY,
    AM_CONCURRENCY_LIMT,
    AM_POLLING_INTERVAL,
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
    max_retries=10,
)
def archivematica(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Submit the SIP of the passed Archive to Archivematica
    preparing the call to the Archivematica API
    Once done, spawn a periodic task to check on the progress
    """
    current_step = Step.objects.get(pk=step_id)
    archive = Archive.objects.get(pk=archive_id)
    try:
        if (res := resource_check(self, current_step, archive)) != 0:
            return res
    except Retry as e:
        current_step.set_status(Status.WAITING)
        current_step.set_output_data(
            {"message": "Archivematica is busy, retrying soon..."}
        )
        raise e

    path_to_sip = archive.path_to_sip

    logger.info(f"Starting archiving {path_to_sip}")

    sip_directory = ntpath.basename(path_to_sip)
    # Path to SIP inside Archivematica transfer source directory
    archivematica_dst = os.path.join(
        "/",
        sip_directory,
    )

    # Adds an _ between Archive and the id because archivematica messes up with spaces
    transfer_name = (
        archive.source + "__" + archive.recid + "_Archive_" + str(archive_id)
    )
    if len(transfer_name) > 50:  # AM has a limit of 50 chars for transfer names
        transfer_name = "Archive_" + str(archive_id)

    # Set up the AMClient to interact with the AM configuration provided in the settings
    am = get_am_client()
    am.transfer_directory = archivematica_dst
    am.transfer_name = transfer_name

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
                    "transfer_name": transfer_name,
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
                am.unit_uuid = message["id"]
                executed_jobs = am.get_jobs()
                logger.debug(
                    f"Executed jobs for given id({message['id']}): {executed_jobs}"
                )
                if executed_jobs != 1 and len(executed_jobs) > 0:
                    is_failed = False
                    am_status = {
                        "status": "PROCESSING",
                        "microservice": "Waiting for archivematica to continue the processing",
                    }
                    logger.info(
                        f"Archivematica package has executed jobs ({len(executed_jobs)}) - waiting for the continuation of the processing"
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
    am_status["transfer_name"] = json.loads(step.output_data)["transfer_name"]

    logger.info(f"Status for {step_id} is: {status}")

    # Needs to validate both because just status=complete does not guarantee that aip is stored
    if status == "COMPLETE" and microservice == "Remove the processing directory":
        try:
            handle_completed_am_package(
                self, task_name, am, step, am_status, archive_id, api_key
            )
        except Exception as e:
            logger.warning(
                f"Error while archiving {step.id}. Archivematica error while querying AIP details: {str(e)}"
            )
            remove_periodic_task_on_failure(
                task_name, step, {"status": "FAILED", "microservice": str(e)}
            )

    elif status == "FAILED" or status == "REJECTED":
        remove_periodic_task_on_failure(task_name, step, am_status)

    elif status == "USER_INPUT":
        # this should not be possible with the automated pipeline but it happens sometimes
        logger.error(
            f"Package requires user input for step {step.id} - automatic pipeline failed"
        )
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


def resource_check(task, current_step, archive):
    if archive.sip_size == 0:
        archive.update_sip_size()
    if archive.sip_size > AGGREGATED_FILE_SIZE_LIMIT:
        return set_and_return_error(
            current_step,
            f"SIP exceeds the Archivematica file size limit ({AGGREGATED_FILE_SIZE_LIMIT // (1024**3)}GB).",
        )
    try:
        with transaction.atomic():
            current_am_steps = Step.objects.select_for_update().filter(
                step_name=StepName.ARCHIVE,
                status__in=[Status.WAITING, Status.IN_PROGRESS],
                celery_task_id__isnull=False,
            )
            current_am_steps_count = current_am_steps.count()

            total_sip_size = (
                Archive.objects.select_for_update()
                .filter(last_step__in=current_am_steps)
                .aggregate(total=Sum("sip_size"))["total"]
                or 0
            )

            if (
                current_am_steps_count >= AM_CONCURRENCY_LIMT
                or total_sip_size + archive.sip_size > AGGREGATED_FILE_SIZE_LIMIT
            ):
                if task.request.retries >= task.max_retries:
                    return set_and_return_error(
                        current_step,
                        "Archivematica max retries exceeded. Try again later.",
                    )

                retry_interval = 10 * (task.request.retries + 1)

                exc_message = (
                    "Archivematica concurrency limit reached."
                    if current_am_steps_count >= AM_CONCURRENCY_LIMT
                    else "Archivematica aggregated file size limit reached."
                )
                raise task.retry(
                    countdown=60 * retry_interval,
                    exc=Exception(exc_message),
                )
            current_step.set_status(Status.WAITING)
            current_step.set_task(task.request.id)
            return 0
    except OperationalError as e:
        if "deadlock detected" in str(e):
            if task.request.retries >= task.max_retries:
                return set_and_return_error(
                    current_step, "Archivematica max retries exceeded. Try again later."
                )

            retry_interval = 2 * (task.request.retries + 1)
            logger.warning(f"Deadlock detected, retrying in {retry_interval} seconds")
            raise task.retry(
                countdown=60 * retry_interval,
                exc=e,
            )
        else:
            raise


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
    )


def get_task_name(step):
    output_data = json.loads(step.output_data)
    if not output_data or "transfer_name" not in output_data:
        raise Exception(
            f"Step {step.id} output data is missing the required 'transfer_name' field."
        )
    return f"AM Status for step: {step.id}, package: {output_data['transfer_name']}"


@shared_task(
    name="callback_package",
    bind=True,
    ignore_result=True,
)
def callback_package(self, package_name):
    logger.info(f"Callback for package {package_name} received.")
    periodic_task = PeriodicTask.objects.filter(name__endswith=package_name)
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
