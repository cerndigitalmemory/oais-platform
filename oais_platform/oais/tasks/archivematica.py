import json
import ntpath
import os

import requests
from amclient import AMClient
from amclient.errors import error_codes, error_lookup
from celery import shared_task, states
from celery.utils.log import get_task_logger
from django.db.models import Sum
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from oais_platform.oais.models import Archive, Status, Step
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
    if (res := resource_check(self, current_step, archive)) != 0:
        return res

    path_to_sip = archive.path_to_sip

    logger.info(f"Starting archiving {path_to_sip}")

    # Set task id
    current_step.set_task(self.request.id)

    sip_directory = ntpath.basename(path_to_sip)
    # Path to SIP inside Archivematica transfer source directory
    archivematica_dst = os.path.join(
        "/",
        sip_directory,
    )

    # Adds an _ between Archive and the id because archivematica messes up with spaces
    transfer_name = sip_directory + "::Archive_" + str(archive_id)

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
            create_check_am_status(package, current_step, archive_id, api_key)
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

    return {"status": 0, "errormsg": "Uploaded to Archivematica"}


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
    task_name = f"Archivematica status for step: {step_id}"

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

    elif status == "FAILED":
        remove_periodic_task_on_failure(task_name, step, am_status)

    elif status == "PROCESSING" or status == "COMPLETE":
        step.set_output_data(am_status)
        step.set_status(Status.IN_PROGRESS)
    else:
        step.set_output_data(am_status)


def resource_check(task, current_step, archive):
    if archive.sip_size > AGGREGATED_FILE_SIZE_LIMIT:
        return set_and_return_error(
            current_step,
            f"SIP exceeds the Archivematica file size limit ({AGGREGATED_FILE_SIZE_LIMIT // (1024**3)}GB).",
        )

    current_am_tasks_qs = PeriodicTask.objects.filter(
        task="check_am_status", enabled=True
    )
    current_am_tasks_count = current_am_tasks_qs.count()

    archive_ids = list(current_am_tasks_qs.values_list("args", flat=True))
    archive_ids = [json.loads(arg)[1] for arg in archive_ids]

    total_sip_size = (
        Archive.objects.filter(pk__in=archive_ids).aggregate(total=Sum("sip_size"))[
            "total"
        ]
        or 0
    )

    if (
        current_am_tasks_count >= AM_CONCURRENCY_LIMT
        or total_sip_size + archive.sip_size > AGGREGATED_FILE_SIZE_LIMIT
    ):
        if task.request.retries >= task.max_retries:
            return set_and_return_error(
                current_step, "Archivematica max retries exceeded. Try again later."
            )

        retry_interval = 10 * (task.request.retries + 1)
        current_step.set_output_data(
            {"message": f"Archivematica is busy, retrying in {retry_interval} minutes."}
        )

        exc_message = (
            "Archivematica concurrency limit reached."
            if current_am_tasks_count >= AM_CONCURRENCY_LIMT
            else "Archivematica aggregated file size limit reached."
        )
        raise task.retry(
            countdown=60 * retry_interval,
            exc=Exception(exc_message),
        )
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


def create_check_am_status(package, step, archive_id, api_key):
    step.set_status(Status.WAITING)
    # overwrite the start date so the waiting limit is counted from here
    step.set_start_date()
    # Create the scheduler
    schedule, _ = IntervalSchedule.objects.get_or_create(
        every=AM_POLLING_INTERVAL, period=IntervalSchedule.MINUTES
    )
    # Spawn a periodic task to check for the status of the package on AM
    PeriodicTask.objects.create(
        interval=schedule,
        name=f"Archivematica status for step: {step.id}",
        task="check_am_status",
        args=json.dumps([package, step.id, archive_id, api_key]),
        expire_seconds=AM_POLLING_INTERVAL * 60.0,
    )


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
        logger.error(f"AIP package with UUID {uuid} not found on {AM_SS_URL}")
        # If the path artifact is not complete try again
        step.set_status(Status.IN_PROGRESS)
        step.set_output_data(am_status)
