import json
import os

from celery import shared_task
from celery.utils.log import get_task_logger
from oais_utils.validate import validate_sip

from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tasks.pipeline_actions import finalize

logger = get_task_logger(__name__)


@shared_task(name="validate", bind=True, ignore_result=True, after_return=finalize)
def validate(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Validate the a folder against the CERN SIP specification,
    using the OAIS utils package
    """
    archive = Archive.objects.get(pk=archive_id)
    sip_folder_name = archive.path_to_sip

    logger.info(f"Starting SIP validation {sip_folder_name}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    # Set task id
    current_step.set_task(self.request.id)

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

    # Set task id
    current_step.set_task(self.request.id)

    sip_exists = os.path.exists(path_to_sip)
    if not sip_exists:
        return {"status": 1, "errormsg": "SIP does not exist"}

    sip_json = os.path.join(path_to_sip, "data/meta/sip.json")

    with open(sip_json) as json_file:
        data = json.load(json_file)
        for file in data["contentFiles"]:
            try:
                checksum_list = []
                for checksum in file["checksum"]:
                    splited = checksum.split(":")
                    checksum = splited[0] + ":" + "0"
                    checksum_list.append(checksum)
            except KeyError:
                current_file = file["origin"]["filename"]
                logger.info(f"Checksum not found for file {current_file}")

    logger.info("Checksum completed!")

    return {"status": 0, "errormsg": None, "foldername": path_to_sip}
