import os

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "oais_platform.settings")

import django
django.setup()
from oais_platform.oais.services import add_periodic_task

from celery import Celery
from django_celery_beat.models import IntervalSchedule

app = Celery("oais_platform")

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


@app.on_after_finalize.connect
def set_up_periodic_tasks(sender, **kwargs):
    # for testing purposes
    print("_____ON AFTER FINALIZE EMITTED_____")

    # we will probably want to change this to a ContrabSchedule
    # to specify the day and time we want to execute the cleanup at
    # something like: Tuedays and Saturdays at 5AM maybe?
    # i think this will be easier to manage
    # rather than every x days (the time (hour and minute of the day)
    # then depends of the time the first iteration's launch time)

    # schedule = IntervalSchedule.objects.create(
    #     every=int(PERIODIC_SCAN_INTERVAL), period=IntervalSchedule.HOURS
    # )

    # for testing purposes, trigger it every 10 secs

    schedule = IntervalSchedule.objects.create(
        every=10, period=IntervalSchedule.SECONDS
    )

    add_periodic_task(schedule, "UploadJob cleanup", "uploadjob_cleanup")
