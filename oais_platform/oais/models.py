from django.contrib.auth.models import User
from django.db import models
from django.utils import timezone
from datetime import datetime

# Create your models here.


class Record(models.Model):
    url = models.CharField(max_length=100)
    recid = models.CharField(max_length=50)
    source = models.CharField(max_length=50)

    class Meta:
        unique_together = ["recid", "source"]

class Status(models.IntegerChoices):
    PENDING = 1
    IN_PROGRESS = 2
    FAILED = 3
    COMPLETED = 4
    WAITING_APPROVAL = 5
    REJECTED = 6

class Stages(models.IntegerChoices):
    HARVEST_REQUESTED = 1
    HARVESTING = 2
    VALIDATION_REQUESTED = 3
    CHECKING_REGISTRY = 4
    VALIDATION = 5
    UPLOADING = 6

class Archive(models.Model):
    record = models.ForeignKey(
        Record, on_delete=models.PROTECT, related_name="archives"
    )
    creator = models.ForeignKey(
        User, on_delete=models.PROTECT, null=True, related_name="archives"
    )
    creation_date = models.DateTimeField(default=timezone.now)
    celery_task_id = models.CharField(max_length=50, null=True, default=None)
    status = models.IntegerField(
        choices=Status.choices, default=Status.WAITING_APPROVAL)
    path_to_sip = models.CharField(max_length=100, null=True, default=None)

    class Meta:
        permissions = [
            ("can_access_all_archives", "Can access all the archival requests"),
            ("can_approve_archive", "Can approve an archival request"),
            ("can_reject_archive", "Can reject an archival request"),
        ]

    def set_in_progress(self, task_id):
        self.celery_task_id = task_id
        self.status = Status.IN_PROGRESS
        self.save()

    def set_completed(self):
        self.status = Status.COMPLETED
        self.save()

    def set_failed(self):
        self.status = Status.FAILED
        self.save()

    def set_pending(self):
        self.status = Status.PENDING
        self.save()

    def get_latest_job(self):
        jobs = self.jobs.all().order_by("-start_date")
        return jobs[0]

class Job(models.Model):
    archive = models.ForeignKey(
        Archive, on_delete=models.PROTECT, related_name="jobs")
    celery_task_id = models.CharField(max_length=50, null=True, default=None)
    start_date = models.DateTimeField(default=timezone.now)
    finish_date = models.DateTimeField(default=None, null=True)
    stage = models.IntegerField(
        choices=Stages.choices, default=Stages.HARVEST_REQUESTED)
    status = models.IntegerField(
        choices=Status.choices, default=Status.WAITING_APPROVAL)

    class Meta:
        unique_together = ["archive", "stage","start_date"]

    def set_in_progress(self, task_id):
        self.celery_task_id = task_id
        self.status = Status.IN_PROGRESS
        self.save()

    def set_failed(self):
        self.status = Status.FAILED
        self.finish_date = datetime.now()
        self.save()

    def set_rejected(self):
        self.status = Status.REJECTED
        self.finish_date = datetime.now()
        self.save()

    def set_completed(self):
        self.status = Status.COMPLETED
        self.finish_date = datetime.now()
        self.save()