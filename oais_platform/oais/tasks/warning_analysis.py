import os

from celery import shared_task
from celery.utils.log import get_task_logger

from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tasks.pipeline_actions import finalize
from oais_platform.settings import RECOGNIZED_FILE_EXTENSIONS

logger = get_task_logger(__name__)


@shared_task(
    name="warning_analysis", bind=True, ignore_result=True, after_return=finalize
)
def warning_analysis(self, archive_id, step_id):
    archive = Archive.objects.get(pk=archive_id)
    logger.info(f"Starting warning analysis for Archive {archive.id}")

    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    warnings = step.input_data_json.get("errormsg") or []

    non_blocking_warnings = []
    blocking_warnings = []
    for warning in warnings:
        if warning.get("task") == "Identify file format":
            if has_recognized_extensions(warning.get("filenames")):
                non_blocking_warnings.append(warning)
            else:
                blocking_warnings.append(warning)
        else:
            blocking_warnings.append(warning)

    if blocking_warnings:
        logger.info(
            f"Manual intervention required for Archive {archive.id}: "
            f"{len(blocking_warnings)} blocking warnings"
        )
        if step.input_step:
            step.set_failure_type(step.input_step.failure_type)
        return {
            "status": 1,
            "errormsg": blocking_warnings,
            "non_blocking_warnings": non_blocking_warnings,
        }

    logger.info(f"No blocking warnings for Archive {archive.id}, continuing pipeline")
    return {
        "status": 0,
        "errormsg": None,
        "non_blocking_warnings": non_blocking_warnings,
    }


def has_recognized_extensions(filenames):
    if not filenames:
        return False
    for filename in filenames:
        if not filename:
            return False
        extension = os.path.splitext(filename)[1].lower()
        if extension not in RECOGNIZED_FILE_EXTENSIONS:
            return False
    return True
