import json
import os

from celery import shared_task
from celery.utils.log import get_task_logger
from fs.errors import ResourceNotFound
from oais_utils.validate import compute_hash, validate_sip

from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tasks.pipeline_actions import finalize

logger = get_task_logger(__name__)


@shared_task(name="validate", bind=True, ignore_result=True, after_return=finalize)
def validate(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Validate a folder against the CERN SIP specification,
    using the OAIS utils package
    """
    archive = Archive.objects.get(pk=archive_id)
    sip_folder_name = archive.path_to_sip

    logger.info(f"Starting SIP validation {sip_folder_name}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    # Checking registry = checking if the folder exists
    sip_exists = os.path.exists(sip_folder_name)

    if not sip_exists:
        return {"status": 1, "errormsg": "SIP does not exist"}

    # Runs validate_sip from oais_utils
    try:
        result = validate_sip(sip_folder_name)
        if result:
            return {"status": 0, "errormsg": None, "foldername": sip_folder_name}
        else:
            return {"status": 1, "errormsg": "SIP validation failed."}
    except Exception as e:
        logger.error(f"SIP validation failed with exception: {str(e)}")
        return {
            "status": 1,
            "errormsg": f"SIP validation failed with exception: {str(e)}",
        }


@shared_task(name="checksum", bind=True, ignore_result=True, after_return=finalize)
def checksum(self, archive_id, step_id, input_data=None, api_key=None):
    archive = Archive.objects.get(pk=archive_id)
    path_to_sip = archive.path_to_sip

    logger.info(f"Starting checksum validation {path_to_sip}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    sip_exists = os.path.exists(path_to_sip)
    if not sip_exists:
        return {"status": 1, "errormsg": "SIP does not exist"}

    manifest = os.path.join(path_to_sip, "manifest-md5.txt")
    err_msg = None
    try:
        with open(manifest) as manifest_file:
            for line in manifest_file:
                line = line.strip()
                if not line:
                    continue

                try:
                    expected_md5, filename = line.split(maxsplit=1)
                except ValueError:
                    err_msg = f"Malformed manifest line: {line!r}"
                    break
                filename = f"{path_to_sip}/{filename}"
                logger.info(f"Checking file: {filename}")
                try:
                    actual_md5 = compute_hash(filename, alg="md5")
                except (FileNotFoundError, ResourceNotFound):
                    err_msg = f"File not found: {filename}"
                    break
                if actual_md5.lower() != expected_md5.lower():
                    err_msg = f"Checksum mismatch for {filename} expected {expected_md5}, got {actual_md5}"

    except FileNotFoundError:
        err_msg = "Manifest file does not exist"

    if err_msg:
        logger.error(err_msg)
        return {"status": 1, "errormsg": err_msg}

    logger.info("Checksum completed!")

    return {"status": 0, "errormsg": None, "foldername": path_to_sip}
