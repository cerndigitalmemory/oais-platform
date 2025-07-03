import os
import xml.etree.ElementTree as ET

from celery import shared_task
from celery.utils.log import get_task_logger

from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tasks.pipeline_actions import finalize

logger = get_task_logger(__name__)


@shared_task(name="extract_title", bind=True, ignore_result=True, after_return=finalize)
def extract_title(self, archive_id, step_id, input_data=None, api_key=None):
    # For archives without title try to extract it from the metadata
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    sip_folder_name = archive.path_to_sip
    dublin_core_path = "data/meta/dc.xml"
    dublin_core_location = os.path.join(sip_folder_name, dublin_core_path)
    try:
        logger.info(f"Starting extract title from dc.xml for Archive {archive.id}")
        xml_tree = ET.parse(dublin_core_location)
        xml = xml_tree.getroot()
        ns = {
            "dc": "http://purl.org/dc/elements/1.1/",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        }
        title = xml.findall("./dc:dc/dc:title", ns)
        title = title[0].text
        logger.info(f"Title found for Archive {archive.id}: {title}")
        archive.set_title(title)
        return {"status": 0, "errormsg": None}
    except Exception as e:
        logger.warning(
            f"Error while extracting title from dc.xml at {dublin_core_location}: {str(e)}"
        )
        return {
            "status": 1,
            "errormsg": f"Title could not be extracted from Dublin Core file at {dublin_core_location}",
        }
