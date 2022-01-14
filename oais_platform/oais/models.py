from django.contrib.auth.models import User
from django.db import models
from django.utils import timezone
from datetime import datetime

from . import pipeline


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
    An archival process of a single addressable record in a upstream
    source
    """

    id = models.AutoField(primary_key=True)
    source_url = models.CharField(max_length=100)
    recid = models.CharField(max_length=50)
    source = models.CharField(max_length=50)
    creator = models.ForeignKey(
        User, on_delete=models.PROTECT, null=True, related_name="archives"
    )
    timestamp = models.DateTimeField(default=timezone.now)
    last_step = models.ForeignKey(
        # Circular reference, use quoted string used to get a lazy reference
        "Step",
        on_delete=models.PROTECT,
        null=True,
        related_name="last_step",
    )
    path_to_sip = models.CharField(max_length=100)
    next_steps = models.JSONField(max_length=50, default=list)

    class Meta:
        ordering = ["-id"]

    def set_step(self, step_id):
        """
        Set last_step to the given Step
        """
        self.last_step = step_id
        self.save()

    def update_next_steps(self, current_step=None):
        """
        Set next_fields according to the pipeline definition
        """
        if current_step:
            self.next_steps = pipeline.get_next_steps(current_step)
        else:
            self.next_steps = pipeline.get_next_steps(self.last_step.name)
        self.save()


class Step(models.Model):
    """
    A single “processing” step in the archival process
    """

    id = models.AutoField(primary_key=True)
    # The archival process this step is in
    archive = models.ForeignKey(Archive, on_delete=models.PROTECT, related_name="steps")
    name = models.IntegerField(choices=Steps.choices)
    start_date = models.DateTimeField(default=timezone.now)
    finish_date = models.DateTimeField(default=None, null=True)
    status = models.IntegerField(choices=Status.choices, default=Status.NOT_RUN)

    celery_task_id = models.CharField(max_length=50, null=True, default=None)
    input_data = models.TextField(max_length=512, null=True, default=None)
    input_step = models.ForeignKey(
        "self",
        on_delete=models.PROTECT,
        related_name="step",
        null=True,
        blank=True,
    )
    output_data = models.TextField(max_length=512, null=True, default=None)

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
