from django.contrib.auth.models import User
from django.db import models
from django.utils import timezone
from datetime import datetime

# Create your models here.
class Steps(models.IntegerChoices):
    SIP_UPLOAD = 1
    HARVEST = 2
    VALIDATION = 3
    CHECKSUM = 4
    ARCHIVE = 5


class Status(models.IntegerChoices):
    NOT_RUN = 1
    IN_PROGRESS = 2
    FAILED = 3
    COMPLETED = 4
    WAITING_APPROVAL = 5
    REJECTED = 6


class Archive(models.Model):
    """
    Represents an archival process of a single addressable record in a upstream source
    """

    id = models.AutoField(primary_key=True)
    source_url = models.CharField(max_length=100)
    recid = models.CharField(max_length=50)
    source = models.CharField(max_length=50)
    creator = models.ForeignKey(
        User, on_delete=models.PROTECT, null=True, related_name="archives"
    )
    timestamp = models.DateTimeField(default=timezone.now)
    current_status = models.CharField(max_length=50)
    path_to_sip = models.CharField(max_length=100)

    class Meta:
        ordering = ["-id"]

    # Put the id of the last_successful step
    def set_step(self, step_status):
        self.current_status = step_status
        self.save()


class Step(models.Model):
    """
    Represents a single “processing” step in the archival process.
    """

    # The archival process this step is in
    id = models.AutoField(primary_key=True)
    archive = models.ForeignKey(Archive, on_delete=models.PROTECT, related_name="steps")
    name = models.IntegerField(choices=Steps.choices)
    start_date = models.DateTimeField(default=timezone.now)
    finish_date = models.DateTimeField(default=None, null=True)
    status = models.IntegerField(
        choices=Status.choices, default=Status.WAITING_APPROVAL
    )
    celery_task_id = models.CharField(max_length=50, null=True, default=None)
    input_data = models.CharField(max_length=100, null=True, default=None)
    input_step = models.ForeignKey(
        "self",
        on_delete=models.PROTECT,
        related_name="step",
        null=True,
        blank=True,
    )
    output_data = models.CharField(max_length=100, null=True, default=None)

    class Meta:
        permissions = [
            ("can_access_all_archives", "Can access all the archival requests"),
            ("can_approve_archive", "Can approve an archival request"),
            ("can_reject_archive", "Can reject an archival request"),
        ]

    def set_status(self, status):
        self.status = status
        self.save()

    def set_task(self, task_id):
        self.celery_task_id = task_id
        self.save()

    def set_input_step(self, input_step):
        self.input_step = input_step
        self.save()

    def set_output_data(self, data):
        self.output_data = data
        self.save()

    def set_finish_date(self):
        self.finish_date = timezone.now()
        self.save()
