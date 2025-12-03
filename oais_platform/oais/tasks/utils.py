from urllib.parse import urljoin

from celery.utils.log import get_task_logger
from django_celery_beat.models import PeriodicTask

from oais_platform.oais.models import Status, Step
from oais_platform.settings import FILES_URL

logger = get_task_logger(__name__)


def create_step(
    step_name,
    archive,
    input_step_id=None,
    input_data=None,
    user=None,
    harvest_batch=None,
):
    """
    Create a new Step of the desired type
    for the given Archive and spawn Celery tasks for it

    step_name: type of the step
    archive: target Archive
    input_step_id: (optional) step to set as "input" for the new one
    """
    return Step.objects.create(
        archive=archive,
        step_name=step_name,
        input_step_id=input_step_id,
        input_data=input_data,
        status=Status.WAITING,
        initiated_by_user=user,
        initiated_by_harvest_batch=harvest_batch,
    )


def create_path_artifact(name, path, localpath):
    """
    Serialize an "Artifact" object with the given values.
    The "URL" path is built prefixing the FILES_URL setting
    """
    # If the path starts with a slash (e.g. in case of /eos/.. paths)
    #  remove it so we can join it without losing parts of the FILES_URL
    if path[0] == "/":
        non_abs_path = path[1:]

    url = urljoin(FILES_URL, non_abs_path)

    return {
        "artifact_name": name,
        "artifact_path": path,
        "artifact_localpath": localpath,
        "artifact_url": url,
    }


def set_and_return_error(step, errormsg, extra_log=None):
    """
    Set the step as failed and return the error message
    """
    step.set_status(Status.FAILED)
    step.set_finish_date()
    if type(errormsg) is dict:
        step.set_output_data(errormsg)
        return errormsg
    else:
        return_value = {"status": 1, "errormsg": errormsg}
        step.set_output_data(return_value)
        logger.error(errormsg + (f" {extra_log}" if extra_log else ""))
        return return_value


def remove_periodic_task_on_failure(task_name, step, output_data):
    """
    Set step as failed and remove the scheduled task
    """
    set_and_return_error(step, output_data)
    logger.warning(f"Step {step.id} failed. Removing periodic task {task_name}.")

    try:
        periodic_task = PeriodicTask.objects.get(name=task_name)
        periodic_task.delete()
    except PeriodicTask.DoesNotExist as e:
        logger.warning(e)
    except Exception as e:
        logger.error(e)
        return


def add_error_to_tag_description(tag, path, errormsg):
    start_ind = tag.description.find(errormsg)
    if start_ind != -1:
        end_ind = start_ind + len(errormsg) + 1
        tag.set_description(
            tag.description[:end_ind] + path + "," + tag.description[end_ind:]
        )
    else:
        if tag.description.find("ERRORS:") == -1:
            tag.set_description(tag.description + " ERRORS:")
        tag.set_description(tag.description + f" {errormsg}:{path}.")
