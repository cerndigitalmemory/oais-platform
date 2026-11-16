import os

from celery import shared_task, states
from celery.utils.log import get_task_logger

from oais_platform.oais.models import Status, Step
from oais_platform.oais.tasks.archivematica import get_am_failure_type_from_failed_job
from oais_platform.oais.tasks.pipeline_actions import finalize
from oais_platform.oais.tasks.utils import set_and_return_error
from oais_platform.settings import RECOGNIZED_FILE_EXTENSIONS

logger = get_task_logger(__name__)


@shared_task(name="warning_analysis", bind=True, ignore_result=True)
def warning_analysis(self, archive_id, step_id):
    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    try:
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
    except Exception as err:
        logger.error(f"Error while analysing warnings for step {step.id}: {err}")
        set_and_return_error(step, str(err))
        return

    if blocking_warnings:
        logger.info(
            f"Manual intervention required for archive {archive_id}: "
            f"{len(blocking_warnings)} blocking warnings"
        )
        step.set_failure_type(
            get_am_failure_type_from_failed_job(blocking_warnings[0].get("task"))
        )
        step.set_output_data(
            {
                "status": 1,
                "errormsg": blocking_warnings,
                "non_blocking_warnings": non_blocking_warnings,
            }
        )
        step.set_status(Status.COMPLETED_WITH_WARNINGS)
        step.set_finish_date()
        return

    logger.info(f"No blocking warnings for archive {archive_id}, continuing pipeline")
    finalize(
        self=self,
        current_status=states.SUCCESS,
        retval={
            "status": 0,
            "errormsg": None,
            "non_blocking_warnings": non_blocking_warnings,
        },
        task_id=None,
        args=[archive_id, step_id, None],
        kwargs=None,
        einfo=None,
    )


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
