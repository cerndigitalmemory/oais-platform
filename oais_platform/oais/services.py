import os
import shutil
import json

from celery import shared_task
from celery.utils.log import get_task_logger
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from django.utils import timezone

from oais_platform.oais.models import UJStatus, UploadJob
from oais_platform.settings import (JOB_EXPIRY_TIME)

logger = get_task_logger(__name__)


# TASK MANAGEMENT


def is_task_set_up(task):
    """
    Scans the PeriodicTask table for possbile task name match.
    Returns true if match is found, false otherwise.
    """

    for pt in PeriodicTask.objects.all():
        if pt.task == task:
            return True

    return False


def add_periodic_task(schedule, name, task, args=None, expiry=None):
    """
    Adds a periodic task to the the beat schedule given:
        - schedule: an IntervalSchedule object
        - name: arbitrary name
        - task: name of the task (actual name of the method)
        - args: list of args the task accepts
        - expiry: a datetime object, if any
    Makes sure the task is not already set up.
    """

    if is_task_set_up(task):
        return

    PeriodicTask.objects.create(
        interval=schedule,
        name=name,
        task=task,
        args=json.dumps(args),
        expires=expiry
    )


# SERVICES LOGIC


@shared_task(name="uploadjob_cleanup", bind=True, ignore_result=True)
def uploadjob_cleanup(self):
    """
    Cleans the UploadJob table. This includes: removing the corresponding temporary directory as well as the entry in the db
    """

    logger.info("Started UploadJob cleanup")

    for uj in UploadJob.objects.all():
        status = uj.status

        if status == UJStatus.SUCCESS or status == UJStatus.FAIL:
            # extra layer in case it was not deleted before
            if os.path.exists(uj.tmp_dir):
                shutil.rmtree(uj.tmp_dir)

            uj.delete()

        elif status == UJStatus.PENDING:
            diff_hrs = (timezone.now() - uj.timestamp).total_seconds() / 3600.0

            if diff_hrs > int(JOB_EXPIRY_TIME):
                if os.path.exists(uj.tmp_dir):
                    shutil.rmtree(uj.tmp_dir)
                uj.delete()

    logger.info("Finished UploadJob cleanup")
