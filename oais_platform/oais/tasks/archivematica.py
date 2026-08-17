import os
import shutil
from datetime import timedelta
from pathlib import Path

import requests
from amclient import AMClient
from amclient.errors import error_codes, error_lookup
from celery import chord, shared_task, states
from celery.utils.log import get_task_logger
from django.db import models, transaction
from django.db.models import Count
from django.utils import timezone

from oais_platform.celery import app
from oais_platform.oais.enums import TERMINAL_STATUSES, StepFailureType
from oais_platform.oais.exceptions import MaxRetriesExceeded
from oais_platform.oais.models import (
    COMPLETED_STATUSES,
    ArchivematicaInstance,
    Status,
    Step,
    StepName,
    StepType,
)
from oais_platform.oais.tasks.pipeline_actions import create_retry_step, finalize
from oais_platform.oais.tasks.utils import (
    cleanup_empty_path,
    create_path_artifact,
    generate_directory_structure,
    get_failure_type_from_status_code,
    set_and_return_error,
)
from oais_platform.settings import AM_PROCESSING_TIME_LIMIT, AM_WAITING_TIME_LIMIT

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
    current_step.set_start_date()
    archive = current_step.archive
    if (res := resource_check(self, current_step, archive)) != 0:
        return res

    # Create AM client & create required directories and return important paths
    am, transfer_sip_path, archivematica_dst, error = _setup_archiving(current_step)
    if error:
        result = error
        cleanup = (
            am is not None
            and transfer_sip_path is not None
            and transfer_sip_path.exists()
        )
    else:
        result, cleanup = _start_archiving(
            self, current_step, am, transfer_sip_path, archivematica_dst
        )
    if cleanup:
        _cleanup_transfer_sip_path(
            current_step, am.sip_upstream_basepath, transfer_sip_path
        )

    return result


def _setup_archiving(step):
    # Get AM instance config or assign instance if get_am_client not done yet
    am, error = get_am_client(step)

    if am:
        transfer_sip_path, archivematica_dst, error = _create_sip_directory(
            step, am.sip_upstream_basepath
        )
        if not error:
            am.transfer_directory = archivematica_dst
            am.transfer_name = get_transfer_name(step.archive, step)
            return am, transfer_sip_path, archivematica_dst, False
        return am, transfer_sip_path, None, error
    return None, None, None, error


def _create_sip_directory(current_step, sip_base_path):
    transfer_sip_path = None
    try:
        archive = current_step.archive
        path_to_sip = Path(archive.path_to_sip)
        transfer_source_path = Path(
            generate_directory_structure(sip_base_path, archive)
        )
        transfer_sip_path = transfer_source_path / path_to_sip.name
        if not transfer_sip_path.exists():
            shutil.copytree(path_to_sip, transfer_sip_path)
        else:
            logger.info(
                f"Transfer path for Archive step: {current_step.id} for Archive: {archive.id} already exists"
            )
        # Path to SIP inside Archivematica transfer source directory
        archivematica_dst = os.path.join(
            "/",
            transfer_sip_path.relative_to(sip_base_path),
        )
        return transfer_sip_path, archivematica_dst, False
    except Exception as e:
        message = f"Error while preparing Archivematica transfer for Archive step: {current_step.id} for Archive: {archive.id}: {str(e)}"
        return (
            transfer_sip_path,
            None,
            set_and_return_error(
                current_step,
                message,
                {
                    "archivematica_instance": current_step.input_data_json.get(
                        "archivematica_instance"
                    ),
                    "transfer_sip_path": (
                        str(transfer_sip_path)
                        if transfer_sip_path is not None
                        else None
                    ),
                },
            ),
        )


def _start_archiving(
    celery_task, step, am: AMClient, transfer_sip_path, archivematica_dst
):
    logger.info(f"Starting archiving {step.archive.path_to_sip}")

    # Create archivematica package
    logger.info(
        f"Creating archivematica package on Archivematica instance: {am.am_url} at directory {archivematica_dst} for Archive: {step.archive.id}"
    )
    try:
        result = None
        message = None
        errormsg = None
        failure_type = None
        extra_log = None
        package = am.create_package()
        if not isinstance(package, (str, int)) or package not in error_codes:
            step.set_output_data(
                {
                    "status": 0,
                    "details": "Uploaded to Archivematica - waiting for processing",
                    "package_uuid": package["id"],
                    "transfer_name": am.transfer_name,
                    "transfer_sip_path": str(transfer_sip_path),
                    "errormsg": None,
                    "archivematica_instance": step.input_data_json.get(
                        "archivematica_instance"
                    ),
                }
            )
            step.set_status(Status.SUBMITTED)
            step.set_task(celery_task.request.id)
            return step.output_data_json, False
        else:
            """
            The AMClient will return error codes when there was an error in the request to the AM API.
            We can't do much in these cases, a part from suggesting to take a look at the AM logs.
            Check 'amclient/errors' for more information.
            """
            errormsg = error_lookup(package)
            message = f"Error while archiving {step.id}. AM create returned error {package}: {errormsg}"

    except requests.HTTPError as e:
        errormsg = (
            f"Error while archiving {step.id}: status code {e.request.status_code}."
        )
        extra_log = f"HTTPError: {e}"
        failure_type = get_failure_type_from_status_code(e.request.status_code)
    except Exception as e:
        errormsg = f"Error while archiving {step.id}: {str(e)}"

    result = set_and_return_error(
        step,
        errormsg,
        {
            "message": message,
            "archivematica_instance": step.input_data_json.get(
                "archivematica_instance"
            ),
            "transfer_sip_path": str(transfer_sip_path),
        },
        extra_log=extra_log,
        failure_type=failure_type,
    )
    return result, True


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

    am, error = get_am_client(step)
    if error:
        return error

    am_status, failure_type = _get_am_status(am, step)
    _handle_am_status(self, step, am, am_status, failure_type)
    _handle_am_retry(step, am_status)


def _handle_am_retry(step, am_status):
    if not am_status.get("retry", False):
        return

    retry_count = 0
    if step.input_step and step.input_step.step_type.name == StepName.ARCHIVE:
        retry_count = step.input_data_json.get("retry_count", 0)

    am_instance = ArchivematicaInstance.objects.filter(
        name=step.input_data_json.get("archivematica_instance"), enabled=True
    ).first()
    if retry_count + 1 > am_instance.retry_limit:
        logger.warning("Max retries exceeded for failed Archivematica jobs.")
        am_status["retry_count"] = retry_count
        am_status["retry_limit_exceeded"] = True
        am_status["retry"] = False
    else:
        am_status["retry_count"] = retry_count + 1
        am_status["retry"] = True
        logger.info(f"Creating Archivematica retry step for archive {step.archive.id}")
        create_retry_step.apply_async(
            args=(
                step.archive.id,
                step.initiated_by_user.id if step.initiated_by_user else None,
                True,
                StepName.ARCHIVE,
            )
        )

    step.set_output_data(am_status)


def _get_am_status(am, step):

    uuid = step.output_data_json.get("package_uuid", None)

    try:
        failure_type = None
        am_status = None
        if uuid:
            am_status = am.get_unit_status(uuid)
            logger.info(f"Current unit status for {am_status}")
        else:
            failure_type = StepFailureType.MISSING_OUTPUT_DATA
            raise ValueError("No package UUID found in step output data.")
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
        # In any other case make task fail (Archivematica crashed or not responding)
        am_status = {"status": "FAILED", "errormsg": str(e)}

    am_status["transfer_name"] = step.output_data_json.get("transfer_name", None)
    am_status["package_uuid"] = uuid
    am_status["transfer_sip_path"] = step.output_data_json.get(
        "transfer_sip_path", None
    )
    am_status["archivematica_instance"] = step.input_data_json.get(
        "archivematica_instance"
    )
    return am_status, failure_type


def _handle_am_status(celery_task, step, am, am_status, failure_type):

    status = am_status["status"]
    microservice = am_status.get("microservice", None)

    logger.info(f"Status for {step.id} is: {status}")
    error = False
    cleanup = False
    # Needs to validate both because just status=complete does not guarantee that aip is stored
    if status == "COMPLETE" and microservice == "Remove the processing directory":
        cleanup, error = _handle_complete_status(celery_task, step, am, am_status)
    elif status in {"FAILED", "REJECTED"}:
        error = True
        _handle_failed_rejected_status(step, am, am_status, failure_type)
    elif status == "USER_INPUT":
        error = True
        _handle_user_input_status(step, am_status)
    elif status == "PROCESSING" or status == "COMPLETE":
        error = _handle_active_status(step, am_status)
    elif status == "WAITING":
        step.set_status(Status.SUBMITTED)
        step.set_output_data(am_status)
    else:
        logger.warning(
            f"Unknown status from Archivematica: {status}, for step {step.id}"
        )
        step.set_output_data(am_status)
    if step.status in TERMINAL_STATUSES:
        step.set_finish_date()
    if error or cleanup:
        _cleanup_transfer_sip_path(step, am.sip_upstream_basepath)


def _handle_complete_status(celery_task, step, am, am_status):
    try:
        cleanup, error = handle_completed_am_package(celery_task, am, step, am_status)
        return cleanup, error
    except Exception as e:
        failure_type = None
        logger.warning(
            f"Error while archiving {step.id}. Archivematica error while querying AIP details: {str(e)}"
        )
        if isinstance(e, MaxRetriesExceeded):
            failure_type = StepFailureType.PACKAGE_NOT_FOUND
        set_and_return_error(
            step,
            str(e),
            {
                "archivematica_instance": step.input_data_json.get(
                    "archivematica_instance"
                ),
                "transfer_sip_path": step.output_data_json.get(
                    "transfer_sip_path", None
                ),
            },
            status="FAILED",
            failure_type=failure_type,
        )
        return True, True


def _handle_failed_rejected_status(step, am, am_status, failure_type):
    uuid = am_status.get("package_uuid") or am_status.get("uuid")
    if not am_status.get("errormsg", None):
        errors, failure_type = get_executed_jobs(am, uuid, check_for_failed=True)
        ingest_uuid = am_status.get("uuid", None)
        if ingest_uuid and ingest_uuid != uuid:
            errors2, failure_type = get_executed_jobs(
                am, ingest_uuid, check_for_failed=True
            )
            errors.extend(errors2)
        logger.warning(
            f"Archivematica reported {len(errors)} failed jobs for step {step.id}."
        )
        am_status["errormsg"] = errors
        am_status["retry"] = True
    if failure_type == StepFailureType.TIMEOUT:
        am_status["retry"] = True
    set_and_return_error(step, output_data=am_status, failure_type=failure_type)


def _handle_user_input_status(step, am_status):
    # this should not be possible with the automated pipeline but it happens sometimes
    logger.warning(
        f"Package requires user input for step {step.id} - automatic pipeline failed"
    )
    am_status["retry"] = True
    set_and_return_error(
        step,
        "Error: Archivematica requires user input.",
        am_status,
        failure_type=StepFailureType.USER_INPUT_REQUIRED,
    )


def _handle_active_status(step, am_status):
    time_passed = (timezone.now() - step.start_date).total_seconds()
    if time_passed < 60 * AM_PROCESSING_TIME_LIMIT:  # Probably stuck in processing
        step.set_output_data(am_status)
        step.set_status(Status.IN_PROGRESS)
        return False
    else:
        logger.info(
            f"Processing time limit reached ({AM_PROCESSING_TIME_LIMIT} mins) - setting step {step.id} to failed"
        )
        am_status["archivematica_instance"] = step.input_data_json.get(
            "archivematica_instance"
        )
        am_status["retry"] = True
        set_and_return_error(
            step,
            "Error: Archivematica processing time limit reached.",
            am_status,
            failure_type=StepFailureType.TIMEOUT,
        )
        return True


def resource_check(task, current_step, archive):
    if archive.sip_size == 0:
        archive.update_sip_size()
    archive_step_type = StepType.get_by_stepname(StepName.ARCHIVE)
    am_instance = current_step.input_data_json.get("archivematica_instance")
    if archive.sip_size > archive_step_type.size_limit_bytes:
        return set_and_return_error(
            current_step,
            f"SIP exceeds the Archivematica file size limit ({archive_step_type.size_limit_bytes // (1024**3)}GB).",
            failure_type=StepFailureType.SIZE_EXCEEDED,
        )
    with transaction.atomic():
        locked_archive_step_type = StepType.objects.select_for_update().get(
            pk=archive_step_type.id
        )
        instance_current_size_bytes = (
            Step.objects.filter(
                step_type=locked_archive_step_type,
                status__in=[Status.IN_PROGRESS, Status.SUBMITTED],
                input_data_json__archivematica_instance=am_instance,
            ).aggregate(total_size=models.Sum("archive__sip_size"))["total_size"]
            or 0
        )
        if (
            instance_current_size_bytes + archive.sip_size
            > locked_archive_step_type.size_limit_bytes
        ):
            logger.warning(
                f"Archivematica aggregated file size limit reached for instance {am_instance}."
            )
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


def get_am_client(step):

    step_am_instance = step.input_data_json.get("archivematica_instance")
    if not step_am_instance:
        logger.info(
            f"Unable to create AM client, no Archivematica instance set for Archive Step: {step.id} for Archive: {step.archive.id}"
        )
        step.set_status(Status.WAITING)
        return None, step.input_data_json

    am_instance = ArchivematicaInstance.objects.filter(
        name=step_am_instance, enabled=True
    ).first()
    if not am_instance:
        return (
            None,
            set_and_return_error(
                step,
                f"Configuration for set Archivematica instance {step_am_instance} could not be found for Archive Step: {step.id} for Archive: {step.archive.id}",
            ),
        )

    am = ArchivematicaClient()
    try:
        am.am_url = am_instance.url
        am.am_user_name = am_instance.username
        am.am_api_key = am_instance.api_key
        am.ss_url = am_instance.storage_service_url
        am.ss_user_name = am_instance.storage_service_username
        am.ss_api_key = am_instance.storage_service_api_key
        am.processing_config = "automated"
        if not am_instance.transfer_source:
            transfer_source = get_transfer_source(am_instance)
            am_instance.set_transfer_source(transfer_source)
        am.transfer_source = am_instance.transfer_source
        am.aip_upstream_basepath = am_instance.aip_upstream_basepath
        am.sip_upstream_basepath = am_instance.sip_upstream_basepath
        return am, False
    except Exception as e:
        logger.error(
            f"Error while getting AM Client for instance: {am_instance.name} for Archive step: {step.id} for Archive: {step.archive.id}: {str(e)}"
        )

        return (
            None,
            set_and_return_error(
                step,
                str(e),
                {
                    "archivematica_instance": am_instance.name,
                },
            ),
        )


class ArchivematicaClient(AMClient):

    sip_upstream_basepath = None
    aip_upstream_basepath = None
    retry_limit = None

    def __init__(self):
        super().__init__()


def get_transfer_source(am_instance):
    DEFAULT_TRANSFER_DESCRIPTION = "Default transfer source"
    try:
        am = AMClient()
        am.ss_url = am_instance.storage_service_url
        am.ss_user_name = am_instance.storage_service_username
        am.ss_api_key = am_instance.storage_service_api_key

        locations = am.list_storage_locations()
        # Archivematica returns integers for errors
        if not locations or not isinstance(locations, dict):
            raise Exception("Invalid storage locations response.")

    except Exception as exc:
        raise Exception(
            f"Failed to connect to Archivematica Storage Service instance '{am_instance.name}': {exc}"
        ) from exc

    objects = locations.get("objects") or []

    for loc in objects:
        if loc.get("description") == DEFAULT_TRANSFER_DESCRIPTION and loc.get(
            "enabled"
        ):
            return loc.get("uuid")

    raise Exception(
        "Transfer source is not defined, and no enabled location with "
        "description 'Default transfer source' was found for instance "
        f"{am_instance.name}."
    )


def get_executed_jobs(am, unit_uuid, check_for_failed=False):
    am.unit_uuid = unit_uuid
    executed_jobs = am.get_jobs()
    logger.debug(f"Executed jobs for given id({unit_uuid}): {executed_jobs}")
    errors = []
    failure_type = None
    if executed_jobs != 1 and len(executed_jobs) > 0:
        if not check_for_failed:
            return len(executed_jobs)
        seen = set()
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
                    key = (
                        job["name"],
                        job.get("microservice"),
                        f"{am.am_url}/tasks/{job['uuid']}",
                    )

                    if key not in seen:
                        seen.add(key)
                        errors.append(
                            {
                                "task": job["name"],
                                "microservice": job.get("microservice"),
                                "link": f"{am.am_url}/tasks/{job['uuid']}",
                            }
                        )
                        if not failure_type:
                            failure_type = get_am_failure_type_from_failed_job(
                                job["name"]
                            )
            except KeyError:
                logger.warning(
                    f"KeyError while checking executed jobs for {unit_uuid}: {str(job)}"
                )
            except Exception as e:
                logger.warning(
                    f"Error while checking executed jobs for {unit_uuid}: {str(e)}"
                )
        return errors, failure_type
    else:
        if not check_for_failed:
            return 0
        else:
            return [], failure_type


def get_am_failure_type_from_failed_job(job):
    match job:
        case "Scan for viruses in directories", "Scan for viruses on extracted files":
            return StepFailureType.VIRUS_FLAGGED
        case "Extract contents from compressed archives":
            return StepFailureType.EXTRACTION_FAILED
        case "Normalize for preservation", "Validate preservation derivatives":
            return StepFailureType.NORMALIZATION_FAILED
        case _:
            return StepFailureType.AM_JOB_FAILED_OTHER


def _cleanup_transfer_sip_path(step, sip_base_path, transfer_sip_path=None):

    transfer_sip_path = transfer_sip_path or step.output_data_json.get(
        "transfer_sip_path", None
    )
    if not transfer_sip_path:
        logger.info(
            f"Archivematica transfer path unkown for step {step.id}: "
            f"{transfer_sip_path}"
        )
        return

    transfer_sip_path = Path(transfer_sip_path)
    base_path = Path(sip_base_path)

    if not transfer_sip_path.exists():
        logger.info(
            f"Archivematica transfer path already removed for step {step.id}: "
            f"{transfer_sip_path}"
        )
        return

    try:
        if transfer_sip_path == base_path or not transfer_sip_path.is_relative_to(
            base_path
        ):
            raise ValueError(
                f"Refusing to clean path outside {base_path}: {transfer_sip_path}"
            )
        shutil.rmtree(transfer_sip_path)
        cleanup_empty_path(
            transfer_sip_path.parent,
            sip_base_path,
            step.archive.source,
        )
        logger.info(
            f"Cleaned Archivematica transfer path for step {step.id}: {transfer_sip_path}"
        )
    except OSError as e:
        logger.error(
            f"Error deleting Archivematica transfer path for step {step.id}: "
            f"{transfer_sip_path}: {e}"
        )
    except ValueError as e:
        logger.error(
            f"Error deleting Archivematica transfer path for step {step.id}: "
            f"{str(e)}"
        )


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


def handle_completed_am_package(celery_task, am, step, am_status):
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
            "AIP",
            os.path.join(am.aip_upstream_basepath, aip_path),
            aip_path,
        )

        step.set_output_data(am_status)
        if step.archive.path_to_aip != am_status["artifact"]["artifact_path"]:
            step.archive.set_aip_path(am_status["artifact"]["artifact_path"])
            step.archive.save()

            outdate_aip_dependent_steps(step.archive)

        errors, failure_type = get_executed_jobs(
            am, am_status["uuid"], check_for_failed=True
        )
        if errors and len(errors) > 0:
            am_status["errormsg"] = errors
            am_status["retry"] = True
            logger.warning(
                f"Archivematica reported {len(errors)} failed jobs for step {step.id}."
            )
            step.set_status(Status.COMPLETED_WITH_WARNINGS)
            step.set_output_data(am_status)
            step.set_failure_type(failure_type)
            return False, True  # force cleanup, error
        else:
            finalize(
                self=celery_task,
                current_status=states.SUCCESS,
                retval={"status": 0},
                task_id=None,
                args=[step.archive.id, step.id, None],
                kwargs=None,
                einfo=None,
            )
            step.refresh_from_db()
            return True, False  # force cleanup, error
    else:
        retry_limit = 5
        retry_count = step.output_data_json.get("package_retry", 0)
        if retry_count + 1 > retry_limit:
            error_msg = f"AIP package with UUID {uuid} not found on {am.ss_url} after retrying {retry_limit} times."
            logger.error(error_msg)
            raise MaxRetriesExceeded(error_msg)
        else:
            logger.warning(
                f"AIP package with UUID {uuid} not found on {am.ss_url}, retrying..."
            )
            am_status["package_retry"] = retry_count + 1
            step.set_status(Status.IN_PROGRESS)
            step.set_output_data(am_status)
            return False, False  # force cleanup, error


@shared_task(name="archive_failed_count_reset")
def archive_failed_count_reset():
    instances = ArchivematicaInstance.objects.filter(
        enabled=True,
        failed_count__gt=0,
    )
    reset_count = instances.update(failed_count=0)
    if reset_count:
        logger.info(
            f"Reset failed count for {reset_count} enabled Archivematica instance(s)."
        )


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

    if not step_type.enabled:
        logger.info("Archivematica step type is currently disabled.")
        return

    recover_stale_assigned_archivematica_steps()

    submitted_count_by_instance = dict(
        Step.objects.filter(
            step_type=step_type,
            status__in=[Status.ASSIGNED, Status.IN_PROGRESS, Status.SUBMITTED],
            input_data_json__has_key="archivematica_instance",
        )
        .values("input_data_json__archivematica_instance")
        .annotate(count=Count("id"))
        .values_list("input_data_json__archivematica_instance", "count")
    )

    # Calculate & determine capacity per Archivematica instance
    am_instance_task_capacity = {}

    for instance in ArchivematicaInstance.objects.filter(enabled=True):
        instance_capacity = (
            step_type.concurrency_limit
            - submitted_count_by_instance.get(instance.name, 0)
        )
        if instance_capacity > 0:
            am_instance_task_capacity[instance.name] = instance_capacity
        logger.info(
            f"Available capacity for instance {instance.name} is {instance_capacity}."
        )

    if len(am_instance_task_capacity) <= 0:
        logger.info("Maximum number of Archivematica steps currently in progress.")
        return

    waiting_assigned_steps = Step.objects.filter(
        step_type=step_type,
        status=Status.WAITING,
        archive__last_step=models.F("id"),
        input_data_json__has_key="archivematica_instance",
    ).order_by("create_date")[: sum(am_instance_task_capacity.values())]

    if waiting_assigned_steps.exists():
        for step in waiting_assigned_steps:
            am_instance = step.input_data_json.get("archivematica_instance")
            if am_instance not in am_instance_task_capacity:
                continue
            assign_and_start_archivematica_step(
                step, am_instance, am_instance_task_capacity
            )

    else:
        logger.info("No assigned waiting Archivematica steps to start")

    waiting_steps = (
        Step.objects.filter(
            step_type=step_type,
            status=Status.WAITING,
            archive__last_step=models.F(
                "id"
            ),  # It is the last step of the archive, not in pipeline
        )
        .exclude(input_data_json__has_key="archivematica_instance")
        .order_by("create_date")[: sum(am_instance_task_capacity.values())]
    )

    if not waiting_steps.exists():
        logger.info("No waiting Archivematica steps to start")
        return

    logger.info(f"Starting {waiting_steps.count()} waiting Archivematica steps to run.")

    for step in waiting_steps:
        am_instance = get_next_am_instance(am_instance_task_capacity)
        assign_and_start_archivematica_step(
            step, am_instance, am_instance_task_capacity
        )


def recover_stale_assigned_archivematica_steps():
    cutoff = timezone.now() - timedelta(minutes=AM_WAITING_TIME_LIMIT)

    stale_assigned_steps = Step.objects.filter(
        step_type__name=StepName.ARCHIVE,
        status=Status.ASSIGNED,
        start_date__isnull=True,
        input_data_json__assigned_at__lt=cutoff.isoformat(),
    )

    for step in stale_assigned_steps:
        logger.warning(
            f"Requeueing stale assigned Archivematica step {step.id} "
            f"for archive {step.archive.id}."
        )
        if step.celery_task_id:
            app.control.revoke(step.celery_task_id, terminate=True)
        step.set_status(Status.WAITING)
        step.remove_input_data_field("archivematica_instance")
        step.set_task(None)


def assign_and_start_archivematica_step(step, am_instance, am_instance_task_capacity):
    decrement_am_instance_capacity(am_instance_task_capacity, am_instance)
    if step.input_data_json.get("archivematica_instance") != am_instance:
        step.set_input_data_field("archivematica_instance", am_instance)
    step.archive.set_archivematica_instance(am_instance)
    step.set_input_data_field("assigned_at", timezone.now().isoformat())
    step.set_status(Status.ASSIGNED)
    result = archivematica.apply_async(args=[step.id])
    step.set_task(result.id)


def get_next_am_instance(am_instance_task_capacity):
    return min(
        am_instance_task_capacity,
        key=lambda am_instance: (
            -am_instance_task_capacity[am_instance],
            am_instance,
        ),
    )


def decrement_am_instance_capacity(am_instance_task_capacity, am_instance):
    am_instance_task_capacity[am_instance] -= 1
    if am_instance_task_capacity[am_instance] <= 0:
        am_instance_task_capacity.pop(am_instance, None)
