import json
import logging
import os
import shutil

import bagit_create
from celery import shared_task
from celery.utils.log import get_task_logger
from django.db import transaction
from django.db.models import Sum

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.pipeline_actions import finalize
from oais_platform.oais.tasks.utils import create_path_artifact, set_and_return_error
from oais_platform.settings import (
    AGGREGATED_FILE_SIZE_LIMIT,
    BIC_UPLOAD_PATH,
    BIC_WORKDIR,
    SIP_UPSTREAM_BASEPATH,
)

logger = get_task_logger(__name__)

RETRY_INTERVAL_MINUTES = 2


@shared_task(
    name="harvest", bind=True, ignore_result=True, after_return=finalize, max_retries=5
)
def harvest(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Run BagIt-Create to harvest data from upstream, preparing a
    Submission Package (SIP)
    """
    bic_version = bagit_create.version.get_version()
    logger.info(
        f"Starting harvest of Archive {archive_id} using BagIt Create {bic_version}"
    )

    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)

    size = archive.original_file_size
    if not size:
        logger.warning(
            f"Archive {archive.id} does not have file size set, skipping size checks."
        )
        step.set_status(Status.IN_PROGRESS)
    else:
        retry = False
        with transaction.atomic():
            if size > AGGREGATED_FILE_SIZE_LIMIT:
                logger.warning(
                    f"Archive {archive.id} exceeds file size limit ({AGGREGATED_FILE_SIZE_LIMIT // (1024**3)}GB)."
                )
                return set_and_return_error(
                    step, "Record is too large to be harvested."
                )

            total_size = (
                Step.objects.select_for_update()
                .filter(step_name=StepName.HARVEST, status=Status.IN_PROGRESS)
                .aggregate(total_original_size=Sum("archive__original_file_size"))[
                    "total_original_size"
                ]
                or 0
            )

            if size + total_size > AGGREGATED_FILE_SIZE_LIMIT:
                logger.warning(
                    f"Archive {archive.id} exceeds aggregated file size limit "
                    f"({AGGREGATED_FILE_SIZE_LIMIT // (1024**3)}GB)."
                )
                if self.request.retries >= self.max_retries:
                    return {"status": 1, "errormsg": "Max retries exceeded."}
                step.set_status(Status.WAITING)
                step.set_output_data(
                    {
                        "message": f"Retrying in {RETRY_INTERVAL_MINUTES} minutes (aggregated file size limit exceeded)",
                    }
                )
                retry = True
            else:
                step.set_status(Status.IN_PROGRESS)

        if retry:
            raise self.retry(
                exc=Exception(
                    f"Retrying in {RETRY_INTERVAL_MINUTES} minutes (aggregated file size limit exceeded)"
                ),
                countdown=RETRY_INTERVAL_MINUTES * 60,
            )

    if not api_key:
        logger.info(
            f"The given source({archive.source}) might requires an API key which was not provided."
        )

    try:
        bagit_result = bagit_create.main.process(
            recid=archive.recid,
            source=archive.source,
            loglevel=logging.WARNING,
            target=BIC_UPLOAD_PATH,
            token=api_key,
            workdir=BIC_WORKDIR,
        )
    except Exception as e:
        return {"status": 1, "errormsg": str(e)}

    logger.info(bagit_result)

    if error_response := _handle_bagit_error(self, archive_id, step, bagit_result):
        return error_response

    return _handle_successful_bagit(archive, bagit_result)


@shared_task(
    name="upload", bind=True, ignore_result=True, after_return=finalize, max_retries=5
)
def upload(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Run BagIt-Create to prepare a Submission Package (SIP) from a locally uploaded file
    """
    bic_version = bagit_create.version.get_version()
    logger.info(
        f"Starting processing of Archive {archive_id} using BagIt Create {bic_version}"
    )
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    if not input_data:
        return {"status": 1, "errormsg": "Missing input data for step"}
    input_data = json.loads(input_data)

    try:
        bagit_result = bagit_create.main.process(
            recid=archive.recid,
            source=archive.source,
            loglevel=logging.WARNING,
            target=BIC_UPLOAD_PATH,
            source_path=input_data.get("tmp_dir"),
            author=input_data.get("author"),
            workdir=BIC_WORKDIR,
        )
    except Exception as e:
        return {
            "status": 1,
            "errormsg": str(e),
            "tmp_dir": input_data.get("tmp_dir"),
            "author": input_data.get("author"),
        }

    logger.info(bagit_result)

    if bagit_result["status"] == 1:
        bagit_result.update(input_data)
        return bagit_result

    _delete_local_upload(input_data.get("tmp_dir"))

    return _handle_successful_bagit(archive, bagit_result)


def _handle_bagit_error(task, archive_id, step, bagit_result):
    """
    Checks the bagit_result for errors and handles retries for specific HTTP error codes.
    Raises task.retry if a retry is initiated.
    Returns an error dict if max retries is hit or error is not retryable.
    """
    if bagit_result["status"] == 1:
        error_msg = str(bagit_result["errormsg"])
        retry = False
        retry_codes = {
            "429": "Rate limit exceeded.",
            "408": "Request timeout.",
            "502": "Bad gateway",
            "503": "Service unavailable",
            "504": "Gateway timeout",
        }
        if "Metadata request was redirected" in error_msg:
            logger.warning(
                f"Archive {archive_id}: URL was redirected; skipping download."
            )
            return {"status": 1, "errormsg": error_msg}
        elif any(key in error_msg for key in retry_codes):
            logger.error(
                next(retry_codes[key] for key in retry_codes if key in error_msg)
            )
            retry = True

        if retry:
            if task.request.retries >= task.max_retries:
                return {"status": 1, "errormsg": "Max retries exceeded."}

            step.set_status(Status.WAITING)
            step.set_output_data(
                {
                    "status": 0,
                    "errormsg": f"Retrying in {RETRY_INTERVAL_MINUTES} minutes (bagit-create error)",
                }
            )
            raise task.retry(
                exc=Exception(error_msg), countdown=RETRY_INTERVAL_MINUTES * 60
            )

        return {"status": 1, "errormsg": error_msg}

    return


def _handle_successful_bagit(archive, bagit_result):
    """
    Update archive path and size and create the artifact.
    """
    sip_folder_name = bagit_result["foldername"]

    if BIC_UPLOAD_PATH:
        sip_folder_name = os.path.join(BIC_UPLOAD_PATH, sip_folder_name)

    archive.set_path(sip_folder_name)
    archive.update_sip_size()

    # Create a SIP path artifact
    output_artifact = create_path_artifact(
        "SIP", os.path.join(SIP_UPSTREAM_BASEPATH, sip_folder_name), sip_folder_name
    )

    bagit_result["artifact"] = output_artifact

    return bagit_result


def _delete_local_upload(upload_path):
    """
    Deletes the folder and its contents on the specified path.
    """
    if not os.path.exists(upload_path):
        logger.warning(f"Attempted to delete non-existent folder: {upload_path}")
        return
    try:
        shutil.rmtree(upload_path)
    except OSError as e:
        logger.error(f"Error deleting folder {upload_path}: {e}")
