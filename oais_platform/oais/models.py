from django.contrib.auth.models import User
from django.db import models
from django.utils import timezone

# Create your models here.


class Record(models.Model):
    url = models.CharField(max_length=100)
    recid = models.CharField(max_length=50)
    source = models.CharField(max_length=50)

    class Meta:
        unique_together = ["recid", "source"]


class ArchiveStatus(models.IntegerChoices):
    PENDING = 1
    IN_PROGRESS = 2
    FAILED = 3
    COMPLETED = 4
    WAITING_APPROVAL = 5
    REJECTED = 6


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
        choices=ArchiveStatus.choices, default=ArchiveStatus.WAITING_APPROVAL
    )

    class Meta:
        permissions = [
            ("can_access_all_archives", "Can access all the archival requests"),
            ("can_approve_archive", "Can approve an archival request"),
            ("can_reject_archive", "Can reject an archival request"),
        ]

    def set_in_progress(self, task_id):
        self.celery_task_id = task_id
        self.status = ArchiveStatus.IN_PROGRESS
        self.save()

    def set_completed(self):
        self.status = ArchiveStatus.COMPLETED
        self.save()

    def set_failed(self):
        self.status = ArchiveStatus.FAILED
        self.save()
