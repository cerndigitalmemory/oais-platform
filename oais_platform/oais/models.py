import hashlib
import json
import logging
import secrets
from pathlib import Path

from celery import current_app
from cryptography.fernet import Fernet
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ObjectDoesNotExist
from django.db import models, transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from django_celery_beat.models import PeriodicTask

from oais_platform.oais.sources.abstract_source import AbstractSource
from oais_platform.settings import ENCRYPT_KEY, INVENIO_SERVER_URL


class Profile(models.Model):
    # Each profile is linked to a user and identified by the same PK
    #  and it's used to save additional per-user values
    user = models.OneToOneField(User, primary_key=True, on_delete=models.CASCADE)
    department = models.CharField(max_length=10, default=None, null=True)
    system = models.BooleanField(default=False)

    class Meta:
        permissions = [
            ("can_execute_step", "Can execute steps"),
            ("can_upload_file", "Can upload files"),
        ]

        constraints = [
            models.UniqueConstraint(
                fields=["system"],
                condition=models.Q(system=True),
                name="unique_system_user",
            )  # One system user
        ]


@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    # Every time a User is created (post_save), create an attached Profile, too
    if created:
        Profile.objects.create(user=instance)


@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    instance.profile.save()


class Status(models.IntegerChoices):
    NOT_RUN = 1, "NOT_RUN"
    IN_PROGRESS = 2, "IN_PROGRESS"
    FAILED = 3, "FAILED"
    COMPLETED = 4, "COMPLETED"
    WAITING_APPROVAL = 5, "WAITING_APPROVAL"
    REJECTED = 6, "REJECTED"
    WAITING = 7, "WAITING"


class ArchiveState(models.IntegerChoices):
    NONE = 1, "NONE"
    SIP = 2, "SIP"
    AIP = 3, "AIP"


class Archive(models.Model):
    """
    An archival process of a single addressable record in a upstream
    source
    """

    id = models.AutoField(primary_key=True)
    source_url = models.CharField(max_length=100)
    recid = models.CharField(max_length=50)
    source = models.CharField(max_length=50)
    requester = models.ForeignKey(
        User, on_delete=models.PROTECT, null=True, related_name="requested_archives"
    )
    approver = models.ForeignKey(
        User, on_delete=models.PROTECT, null=True, related_name="approved_archives"
    )
    timestamp = models.DateTimeField(default=timezone.now)
    last_modification_timestamp = models.DateTimeField(default=timezone.now)
    last_completed_step = models.ForeignKey(
        # Circular reference, use quoted string used to get a lazy reference
        "Step",
        on_delete=models.SET_NULL,
        null=True,
        related_name="last_completed_step",
    )
    last_step = models.ForeignKey(
        # Circular reference, use quoted string used to get a lazy reference
        "Step",
        on_delete=models.SET_NULL,
        null=True,
        related_name="last_step",
    )
    path_to_sip = models.CharField(max_length=100)
    path_to_aip = models.CharField(max_length=250, null=True)
    pipeline_steps = models.JSONField(default=list)
    manifest = models.JSONField(default=None, null=True)
    staged = models.BooleanField(default=False)
    title = models.TextField(default="")
    restricted = models.BooleanField(default=True)
    # A number we'll increment every time we need to publish a new version on InvenioRDM
    invenio_version = models.IntegerField(default=0)
    # Resource attached to the archive
    resource = models.ForeignKey("Resource", null=True, on_delete=models.CASCADE)
    state = models.IntegerField(choices=ArchiveState.choices, null=True)
    sip_size = models.BigIntegerField(default=0)
    original_file_size = models.BigIntegerField(default=0)
    # Timestamp from the upstream source
    version_timestamp = models.DateTimeField(default=None, null=True)

    class Meta:
        ordering = ["-id"]
        permissions = (
            ("can_approve_all", "Can approve any record and start the pipeline"),
            ("view_archive_all", "Can view all archives"),
            ("can_edit_all", "Can edit all archives"),
        )

    def set_last_completed_step(self, step_id):
        """
        Set last_completed_step to the given Step
        """
        self.last_completed_step_id = step_id
        self.save()

    def set_last_step(self, step_id):
        """
        Set last_step to the given Step
        """
        self.last_step_id = step_id
        self.save()

    def set_archive_manifest(self, manifest):
        """
        Set manifest to the given object
        """
        self.manifest = manifest
        self.save()

    def get_collections(self):
        return self.archive_collections.all()

    def set_unstaged(self, approver=None):
        self.staged = False
        self.approver = approver
        self.save()

    def set_path(self, new_path):
        self.path_to_sip = new_path
        self.save()

    def set_aip_path(self, new_aip_path):
        self.path_to_aip = new_aip_path
        self.save()

    def set_title(self, title):
        self.title = title
        self.save()

    def update_sip_size(self):
        self.sip_size = sum(
            file.stat().st_size for file in Path(self.path_to_sip).rglob("*")
        )
        self.save()

    def set_original_file_size(self, size):
        self.original_file_size = size
        self.save()

    def save(self, *args, **kwargs):
        # If the object is being created right now:
        if not self.pk:
            # Check if there is a Resource with the same source+recid
            try:
                resource = Resource.objects.get(source=self.source, recid=self.recid)
            except ObjectDoesNotExist:
                resource = None

            # If the resource does not exists we create it
            if resource is None:
                resource = Resource.objects.create(source=self.source, recid=self.recid)
                resource.save()

            # The resource now exists, so I attach it to the archive
            self.resource = resource

        self.set_state()
        self.last_modification_timestamp = timezone.now()
        # Normal logic of the save method
        super(Archive, self).save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        # delete all steps related to this archive
        if self.last_completed_step:
            self.last_completed_step.delete()
        if self.last_step:
            self.last_step.delete()
        for id in self.pipeline_steps:
            try:
                step = Step.objects.get(id=id)
                step.delete()
            except Exception:
                pass
        super().delete(*args, **kwargs)

    def set_state(self):
        try:
            is_sip = self.steps.filter(
                step_type__has_sip=True, status=Status.COMPLETED
            ).exists()
            is_aip = self.steps.filter(
                step_type__has_aip=True, status=Status.COMPLETED
            ).exists()

            if is_sip and is_aip:
                state = ArchiveState.AIP
            elif is_sip:
                state = ArchiveState.SIP
            else:
                state = ArchiveState.NONE

            self.state = state
        except Exception:
            self.state = ArchiveState.NONE

    def consume_pipeline(self):
        step_id = self.pipeline_steps.pop(0)
        self.save()

        return step_id

    def add_step_to_pipeline(
        self, step_name, user=None, harvest_batch=None, lock=False
    ):
        archive = self

        try:
            StepType.objects.get(name=step_name)
        except StepType.DoesNotExist:
            raise Exception(f"Invalid Step type: {step_name}")

        with transaction.atomic():

            if lock:
                archive = Archive.objects.select_for_update().get(pk=self.pk)

            if not archive.pipeline_steps:
                archive.pipeline_steps = []

            if len(archive.pipeline_steps) == 0:
                if archive.last_step:
                    input_step_id = archive.last_step.id
                else:
                    input_step_id = None
            else:
                input_step_id = archive.pipeline_steps[-1]

            step = Step.objects.create(
                archive=archive,
                step_name=step_name,
                input_step_id=input_step_id,
                status=Status.WAITING,
                initiated_by_user=user,
                initiated_by_harvest_batch=harvest_batch,
            )

            archive.pipeline_steps.append(step.id)
            archive.save()

    @property
    def is_pushed_to_tape(self):
        return self.steps.filter(
            step_type__name=StepName.PUSH_TO_CTA, status=Status.COMPLETED
        ).exists()

    @property
    def has_notified_source(self):
        return self.steps.filter(
            step_type__name=StepName.NOTIFY_SOURCE, status=Status.COMPLETED
        ).exists()

    @property
    def is_pushed_to_registry(self):
        return self.steps.filter(
            step_type__name=StepName.INVENIO_RDM_PUSH, status=Status.COMPLETED
        ).exists()


class StepName(models.TextChoices):
    FILE_UPLOAD = "FILE_UPLOAD"
    SIP_UPLOAD = "SIP_UPLOAD"
    HARVEST = "HARVEST"
    VALIDATION = "VALIDATION"
    CHECKSUM = "CHECKSUM"
    ARCHIVE = "ARCHIVE"
    EDIT_MANIFEST = "EDIT_MANIFEST"
    INVENIO_RDM_PUSH = "INVENIO_RDM_PUSH"
    ANNOUNCE = "ANNOUNCE"
    PUSH_TO_CTA = "PUSH_TO_CTA"
    EXTRACT_TITLE = "EXTRACT_TITLE"
    NOTIFY_SOURCE = "NOTIFY_SOURCE"


def get_task_names():
    return [
        (task, task)
        for task in current_app.tasks.keys()
        if not task.startswith("celery.")
    ]


class StepType(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=50, choices=StepName.choices, unique=True)
    label = models.CharField(max_length=100)
    description = models.TextField(max_length=250, null=True, default=None)
    task_name = models.CharField(max_length=50, choices=get_task_names, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    failed_count = models.IntegerField(default=0)
    failed_blocking_limit = models.IntegerField(default=None, null=True)
    enabled = models.BooleanField(default=True)
    has_sip = models.BooleanField(default=False)
    has_aip = models.BooleanField(default=False)
    automatic_next_step = models.ForeignKey(
        "self", null=True, on_delete=models.SET_NULL
    )

    @classmethod
    def get_by_stepname(cls, stepname):
        return cls.objects.get(name=stepname)

    def set_enabled(self, enabled):
        self.enabled = enabled
        self.save()

    def increment_failed_count(self):
        self.failed_count += 1
        if (
            self.failed_blocking_limit is not None
            and self.failed_count >= self.failed_blocking_limit
        ):
            self.enabled = False
        self.save()

    def unblock(self):
        self.failed_count = 0
        self.enabled = True
        self.save()


class StepQuerySet(models.QuerySet):
    def filter(self, *args, **kwargs):
        if "step_name" in kwargs:
            step_name = kwargs.pop("step_name")
            try:
                step_type = StepType.get_by_stepname(step_name)
            except StepType.DoesNotExist:
                return self.none()
            kwargs["step_type"] = step_type
        return super().filter(*args, **kwargs)


class StepManager(models.Manager):
    def create(self, *, step_name: StepName, **kwargs):
        # resolve StepType from StepName
        step_type = StepType.get_by_stepname(step_name)
        kwargs["step_type"] = step_type
        return super().create(**kwargs)

    def get_queryset(self):
        return StepQuerySet(self.model, using=self._db)


class Step(models.Model):
    """
    A single “processing” step in the archival process
    """

    id = models.AutoField(primary_key=True)
    # The archival process this step is in
    archive = models.ForeignKey(Archive, on_delete=models.CASCADE, related_name="steps")
    step_type = models.ForeignKey(
        StepType, on_delete=models.PROTECT, related_name="steps", null=True
    )
    create_date = models.DateTimeField(default=timezone.now)
    start_date = models.DateTimeField(default=None, null=True)
    finish_date = models.DateTimeField(default=None, null=True)
    status = models.IntegerField(choices=Status.choices, default=Status.NOT_RUN)

    initiated_by_user = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="steps"
    )
    initiated_by_harvest_batch = models.ForeignKey(
        "HarvestBatch",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="steps",
    )

    celery_task_id = models.CharField(max_length=50, null=True, default=None)
    input_data = models.TextField(null=True, default=None)
    input_step = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        related_name="step",
        null=True,
        blank=True,
    )
    output_data = models.TextField(null=True, default=None)

    objects = StepManager()

    @property
    def is_user_initiated(self):
        """Check if this step was initiated by a user"""
        return self.initiated_by_user is not None

    @property
    def is_batch_initiated(self):
        """Check if this step was initiated by a harvest batch"""
        return self.initiated_by_harvest_batch is not None

    def set_status(self, status):
        self.status = status
        self.save()

        if self.is_batch_initiated:
            # Check if the batch is finished
            with transaction.atomic():
                batch = HarvestBatch.objects.select_for_update().get(
                    pk=self.initiated_by_harvest_batch.id
                )
                if (
                    batch.size - batch.skipped_count == batch.completed
                    or batch.skipped_count == batch.size
                ):
                    batch.set_status(BatchStatus.COMPLETED)
                    logging.info(f"Batch {batch.id} completed")
                elif batch.size - batch.skipped_count == batch.failed:
                    batch.set_status(BatchStatus.FAILED)
                    logging.error(f"Batch {batch.id} failed")
                elif batch.size - batch.skipped_count == batch.completed + batch.failed:
                    batch.set_status(BatchStatus.PARTIALLY_FAILED)
                    logging.warning(f"Batch {batch.id} partially failed")

    def set_task(self, task_id):
        self.celery_task_id = task_id
        self.save()

    def set_input_step(self, input_step):
        self.input_step = input_step
        self.save()

    def set_input_data(self, data):
        self.input_data = json.dumps(data)
        self.save()

    def set_output_data(self, data):
        self.output_data = json.dumps(data)
        self.save()

    def set_finish_date(self):
        self.finish_date = timezone.now()
        self.save()

    def set_start_date(self):
        self.start_date = timezone.now()
        self.save()

    def save(self, *args, **kwargs):
        super(Step, self).save(*args, **kwargs)
        self.archive.save()


class Resource(models.Model):
    """
    A group of attributes that have in common all the Archives that have the same source+ recid pair
    Different Archives refferring to the same upstream source will refer to the same Resource
    """

    id = models.AutoField(primary_key=True)

    # Source and recid (unique pair)
    source = models.CharField(max_length=50)
    recid = models.CharField(max_length=50)

    # Invenio parameters of the first archive that creates a version
    # Parameters needed for creating new versions
    invenio_id = models.CharField(max_length=50)
    invenio_parent_id = models.CharField(
        max_length=150, default=None, blank=True, null=True
    )
    invenio_parent_url = models.CharField(
        max_length=150, default=None, blank=True, null=True
    )

    # Set invenio_id of the first archive that pushes a version to InvenioRDM
    def set_invenio_id(self, invenio_id):
        self.invenio_id = invenio_id
        self.save()

    # Set the values for both fields that need the invenio_parent_id
    def set_invenio_parent_fields(self, invenio_parent_id):
        self.invenio_parent_id = invenio_parent_id
        self.invenio_parent_url = f"{INVENIO_SERVER_URL}/search?q=parent.id:{invenio_parent_id}&f=allversions:true"
        self.save()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["source", "recid"], name="resource_source_recid_unique"
            )
        ]


class Collection(models.Model):
    """
    A collection of multiple archives
    """

    id = models.AutoField(primary_key=True)
    title = models.CharField(max_length=50, null=True, default="Untitled")
    description = models.TextField(max_length=1024, null=True, default=None)
    creator = models.ForeignKey(
        User, on_delete=models.PROTECT, null=True, related_name="collections"
    )
    timestamp = models.DateTimeField(default=timezone.now)
    last_modification_date = models.DateTimeField(default=timezone.now)
    archives = models.ManyToManyField(
        Archive, blank=True, related_name="archive_collections"
    )
    internal = models.BooleanField(default=False)

    class Meta:
        ordering = ["-id"]

    def set_title(self, title):
        self.title = title
        self.save()

    def set_description(self, description):
        max_desc_length = self._meta.get_field("description").max_length
        if len(description) > max_desc_length:
            description = description[0 : max_desc_length - 3] + "..."
        self.description = description
        self.save()

    def set_modification_timestamp(self):
        self.last_modification_date = timezone.now()
        self.save()

    def add_archive(self, archive):
        self.archives.add(archive)
        self.save()

    def remove_archive(self, archive):
        self.archives.remove(archive)
        self.save()


def get_source_classnames():
    return [(cls.__name__, cls.__name__) for cls in AbstractSource.__subclasses__()]


class Source(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=50, null=False, unique=True)
    longname = models.CharField(max_length=100, null=False, unique=True)
    api_url = models.CharField(max_length=250, null=False, unique=True)
    enabled = models.BooleanField(default=True)
    timestamp = models.DateTimeField(default=timezone.now)
    classname = models.CharField(choices=get_source_classnames, null=False)
    has_restricted_records = models.BooleanField(default=True)
    has_public_records = models.BooleanField(default=True)
    how_to_get_key = models.TextField(max_length=500, null=True)
    description = models.TextField(max_length=500, null=True)
    notification_endpoint = models.CharField(max_length=250, null=True)
    notification_enabled = models.BooleanField(default=False)

    class Meta:
        ordering = ("id",)


class ApiKey(models.Model):
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, null=False, related_name="api_key"
    )
    source = models.ForeignKey(Source, on_delete=models.CASCADE, null=False)
    _key = models.TextField(max_length=500, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["user", "source"], name="unique_user_source"
            )
        ]

    def encrypt(self, text):
        f = Fernet(ENCRYPT_KEY)
        return f.encrypt(text.encode()).decode()

    def decrypt(self, ciphertext) -> str:
        f = Fernet(ENCRYPT_KEY)
        return f.decrypt(ciphertext.encode()).decode()

    def get_key(self):
        return self.decrypt(self._key)

    def set_key(self, val):
        self._key = self.encrypt(val)

    key = property(get_key, set_key)


class PersonalAccessTokenManager(models.Manager):
    def create(self, *, token: str, **kwargs):
        # Hash token
        kwargs["token_hash"] = PersonalAccessToken.hash(token)
        return super().create(**kwargs)


class PersonalAccessToken(models.Model):
    name = models.CharField(max_length=100)
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name="personal_access_tokens"
    )
    token_hash = models.CharField(max_length=255, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_used_at = models.DateTimeField(null=True)
    expires_at = models.DateTimeField(null=True)
    revoked = models.BooleanField(default=False)

    objects = PersonalAccessTokenManager()

    class Meta:
        unique_together = [("user", "name")]

    @classmethod
    def hash(cls, token):
        return hashlib.sha256(token.encode()).hexdigest()

    @classmethod
    def generate_token(cls):
        return secrets.token_urlsafe(32)


class FilterType(models.TextChoices):
    CREATED = "created"
    UPDATED = "updated"


class ScheduledHarvest(models.Model):
    """
    This model represents a scheduled harvest job that can be periodically executed.
    The parameters will be used for the next harvest run.
    """

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, null=False, unique=True)
    source = models.ForeignKey(
        Source, on_delete=models.PROTECT, null=False, related_name="scheduled_harvests"
    )
    scheduling_task = models.ForeignKey(
        PeriodicTask, on_delete=models.SET_NULL, null=True
    )
    enabled = models.BooleanField(default=False)
    pipeline = ArrayField(
        models.CharField(choices=StepName.choices), blank=True, default=list
    )
    filter_type = models.CharField(
        choices=FilterType.choices, default=FilterType.UPDATED
    )
    grace_period_days = models.PositiveIntegerField(default=0, null=False)
    batch_size = models.PositiveIntegerField(default=100, null=False)
    batch_delay_minutes = models.PositiveIntegerField(default=15, null=False)

    def set_enabled(self, enabled):
        self.enabled = enabled
        self.save()

    def set_pipeline(self, pipeline):
        self.validate_pipeline(pipeline)
        self.pipeline = pipeline
        self.save()


class HarvestRun(models.Model):
    """
    This model represents a single execution of a ScheduledHarvest.
    It contains all the parameters used for the executed harvest.
    """

    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    source = models.ForeignKey(
        Source, on_delete=models.PROTECT, null=False, related_name="harvest_runs"
    )
    collection = models.ForeignKey(
        Collection, on_delete=models.PROTECT, null=True, related_name="harvest_runs"
    )
    scheduled_harvest = models.ForeignKey(
        ScheduledHarvest,
        on_delete=models.SET_NULL,
        null=True,
        related_name="harvest_runs",
    )
    pipeline = ArrayField(
        models.CharField(choices=StepName.choices), blank=True, default=list
    )
    query_start_time = models.DateTimeField(default=None, null=True)
    query_end_time = models.DateTimeField(default=None, null=True)
    filter_type = models.CharField(
        choices=FilterType.choices, default=FilterType.UPDATED
    )
    grace_period_days = models.PositiveIntegerField(default=0, null=False)
    batch_size = models.PositiveIntegerField(default=100, null=False)
    batch_delay_minutes = models.PositiveIntegerField(default=15, null=False)

    def get_next_pending_batch(self):
        return (
            self.batches.filter(status=BatchStatus.PENDING)
            .order_by("batch_number")
            .first()
        )

    def set_collection(self, collection):
        self.collection = collection
        self.save()

    @property
    def archive_count(self):
        if self.collection is None or self.collection.archives is None:
            return 0
        return self.collection.archives.count()

    @property
    def size(self):
        return sum(batch.size for batch in self.batches.all())

    @property
    def skipped_count(self):
        return sum(batch.skipped_count for batch in self.batches.all())


class BatchStatus(models.TextChoices):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    BLOCKED = "BLOCKED"
    PARTIALLY_FAILED = "PARTIALLY_FAILED"
    FAILED = "FAILED"


class HarvestBatch(models.Model):
    """
    This model represents a single batch of records to be harvested in a HarvestRun.
    batch_number is unique per HarvestRun and represents the order of execution.
    If a batch is failed or blocked manually further batches will not be executed.
    """

    id = models.AutoField(primary_key=True)
    batch_number = models.PositiveIntegerField()
    status = models.CharField(choices=BatchStatus.choices, default=BatchStatus.PENDING)
    created_at = models.DateTimeField(auto_now_add=True)
    records = models.JSONField(default=list)
    harvest_run = models.ForeignKey(
        HarvestRun, on_delete=models.CASCADE, related_name="batches"
    )
    skipped_count = models.PositiveIntegerField(default=0)

    @property
    def size(self):
        return len(self.records or [])

    @property
    def failed(self):
        last_non_waiting_step = (
            Step.objects.filter(
                archive=models.OuterRef("pk"), initiated_by_harvest_batch=self
            )
            .exclude(status=Status.WAITING)
            .order_by("-create_date")
            .values("status")[:1]
        )

        archives_with_status = self.archives.annotate(
            batch_status=models.Subquery(last_non_waiting_step)
        )

        return archives_with_status.filter(batch_status=Status.FAILED).count()

    @property
    def completed(self):
        last_step = (
            Step.objects.filter(
                archive=models.OuterRef("pk"), initiated_by_harvest_batch=self
            )
            .order_by("-create_date")
            .values("status")[:1]
        )

        archives_with_status = self.archives.annotate(
            batch_status=models.Subquery(last_step)
        )

        return archives_with_status.filter(batch_status=Status.COMPLETED).count()

    @property
    def archives(self):
        return Archive.objects.filter(steps__initiated_by_harvest_batch=self).distinct()

    class Meta:
        unique_together = ("harvest_run", "batch_number")
        ordering = ["batch_number"]

    def set_status(self, status):
        self.status = status
        self.save()

    def increase_skipped_count(self):
        self.skipped_count += 1
        self.save()
