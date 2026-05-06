import hashlib
import os
from pathlib import Path
from urllib.parse import urljoin

from celery.utils.log import get_task_logger
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.models import ApiKey, Profile, Status, Step
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
        input_data_json=input_data,
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


def set_and_return_error(step, errormsg, extra_log=None, failure_type=None):
    """
    Set the step as failed and return the error message
    """
    if failure_type and not step.failure_type:
        step.set_failure_type(failure_type)
    else:
        step.set_failure_type(StepFailureType.OTHER)
    step.set_status(Status.FAILED)
    step.set_finish_date()
    if type(errormsg) is dict:
        step.set_output_data(errormsg)
        return_value = errormsg
    else:
        return_value = {"status": 1, "errormsg": errormsg}
        step.set_output_data(return_value)
        logger.error(errormsg + (f" {extra_log}" if extra_log else ""))
    return return_value


def remove_periodic_task_on_failure(task_name, step, output_data, failure_type=None):
    """
    Set step as failed/timed out and remove the scheduled task
    """
    set_and_return_error(step, output_data, failure_type=failure_type)
    logger.warning(f"Step {step.id} failed. Removing periodic task {task_name}.")

    try:
        remove_periodic_task_if_exists(task_name)
    except Exception as e:
        logger.error(e)
        return


def remove_periodic_task_if_exists(task_name):
    if PeriodicTask.objects.filter(name=task_name).exists():
        try:
            periodic_task = PeriodicTask.objects.get(name=task_name)
            periodic_task.delete()
        except PeriodicTask.DoesNotExist:
            logger.info(f"Task {task_name} already removed")


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


def generate_directory_structure(base_path, archive):
    unique_id = hashlib.md5(archive.title.encode()).hexdigest()
    segments = [unique_id[i : i + 4] for i in range(0, len(unique_id), 4)]
    full_path = os.path.join(base_path, archive.source, *segments)
    os.makedirs(full_path, exist_ok=True)
    return full_path


def cleanup_empty_path(path_to_clean, base_path, source):
    current = Path(path_to_clean)
    limit = Path(base_path) / source

    for folder in [current] + list(current.parents):
        if folder == limit or not folder.is_relative_to(limit):
            break
        try:
            os.rmdir(folder)
        except OSError:
            logger.warning(f"Not cleaning up directory {folder} as it is not empty")
            break


def get_api_key_for_step(step, force_system=False):
    api_key = None
    if step.initiated_by_harvest_batch or force_system:
        try:
            user = Profile.objects.get(system=True).user
            api_key = ApiKey.objects.get(
                source__name=step.archive.source, user=user
            ).key
        except Profile.DoesNotExist:
            logger.error("System user does not exist.")
            return
        except ApiKey.DoesNotExist:
            logger.warning(
                f"System user({user.username}) does not have API key set for the given source."
            )
    elif step.initiated_by_user:
        try:
            api_key = ApiKey.objects.get(
                source__name=step.archive.source, user=step.initiated_by_user
            ).key
        except ApiKey.DoesNotExist:
            logger.warning(
                f"User({step.initiated_by_user.username}) does not have API key set for the given source."
            )
    return api_key


def get_interval_schedule(every, period):
    # to ensure no duplicate IntervalSchedule is created when executing a large amount of tasks simultaneously
    try:
        schedule, _ = IntervalSchedule.objects.get_or_create(every=every, period=period)
        return schedule
    except Exception:
        return IntervalSchedule.objects.filter(every=every, period=period).first()


def get_failure_type_from_status_code(status_code):
    try:
        return StepFailureType[f"HTTP_{status_code}"]
    except KeyError:
        return StepFailureType.HTTP_OTHER
