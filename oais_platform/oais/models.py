import json
import logging

from cryptography.fernet import Fernet
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ObjectDoesNotExist
from django.db import models, transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone

from oais_platform.oais.sources.abstract_source import AbstractSource
from oais_platform.settings import ENCRYPT_KEY, INVENIO_SERVER_URL

from . import pipeline


class Profile(models.Model):
    # Each profile is linked to a user and identified by the same PK
    #  and it's used to save additional per-user values
    #  (e.g. configuration, preferences, API tokens)
    #  accessible as user.profile.VALUE
    user = models.OneToOneField(User, primary_key=True, on_delete=models.CASCADE)
    # make sure default here is a callable returning a list
    cern_roles = ArrayField(models.CharField(max_length=500), default=list, blank=True)

    class Meta:
        permissions = [
            ("can_view_system_settings", "Can view System Settings"),
        ]

    def update(self, data):
        for key in data:
            setattr(self, key, data[key])
        self.save()

    def update_roles(self, data):
        setattr(self, "cern_roles", data)
        self.save()


@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    # Every time a User is created (post_save), create an attached Profile, too
    if created:
        Profile.objects.create(user=instance)


@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    instance.profile.save()


class Steps(models.IntegerChoices):
    SIP_UPLOAD = 1
    HARVEST = 2
    VALIDATION = 3
    CHECKSUM = 4
    ARCHIVE = 5
    EDIT_MANIFEST = 6
    INVENIO_RDM_PUSH = 7
    ANNOUNCE = 8
    PUSH_TO_CTA = 9
    EXTRACT_TITLE = 10
    NOTIFY_SOURCE = 11


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
    creator = models.ForeignKey(
        User, on_delete=models.PROTECT, null=True, related_name="archives"
    )
    timestamp = models.DateTimeField(default=timezone.now)
    last_modification_timestamp = models.DateTimeField(default=timezone.now)
    last_completed_step = models.ForeignKey(
        # Circular reference, use quoted string used to get a lazy reference
        "Step",
        on_delete=models.PROTECT,
        null=True,
        related_name="last_completed_step",
    )
    last_step = models.ForeignKey(
        # Circular reference, use quoted string used to get a lazy reference
        "Step",
        on_delete=models.PROTECT,
        null=True,
        related_name="last_step",
    )
    path_to_sip = models.CharField(max_length=100)
    path_to_aip = models.CharField(max_length=250, null=True)
    pipeline_steps = models.JSONField(default=list)
    manifest = models.JSONField(default=None, null=True)
    staged = models.BooleanField(default=False)
    title = models.CharField(max_length=255, default="")
    restricted = models.BooleanField(default=True)
    # A number we'll increment every time we need to publish a new version on InvenioRDM
    invenio_version = models.IntegerField(default=0)
    # Resource attached to the archive
    resource = models.ForeignKey("Resource", null=True, on_delete=models.CASCADE)
    state = models.IntegerField(choices=ArchiveState.choices, null=True)

    class Meta:
        ordering = ["-id"]
        permissions = (
            ("grant_view_right", "Grant view right"),
            ("can_unstage", "Can unstage a record and start the pipeline"),
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

    def set_unstaged(self):
        self.staged = False
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

    def set_state(self):
        try:
            steps = self.steps.all().order_by("-start_date", "-create_date")
            state = ArchiveState.NONE
            for step in steps:
                if step.status == Status.COMPLETED:
                    if step.name == Steps.CHECKSUM:
                        state = ArchiveState.SIP
                        break
                    elif step.name == Steps.ARCHIVE:
                        state = ArchiveState.AIP
                        break
            self.state = state
        except Exception:
            self.state = ArchiveState.NONE

    def consume_pipeline(self):
        step_id = self.pipeline_steps.pop(0)
        self.save()

        return step_id

    def add_step_to_pipeline(self, step_name, lock=False):
        archive = self

        if step_name not in Steps:
            raise Exception("Invalid Step type")

        with transaction.atomic():

            if lock:
                archive = Archive.objects.select_for_update().get(pk=self.pk)

            if not archive.pipeline_steps:
                archive.pipeline_steps = []

            if len(archive.pipeline_steps) == 0:
                if archive.last_step:
                    input_step_id = archive.last_step.id
                    prev_step_name = archive.last_step.name
                else:
                    input_step_id = None
                    prev_step_name = None
            else:
                input_step_id = archive.pipeline_steps[-1]
                prev_step_name = Step.objects.get(pk=input_step_id).name

            if input_step_id and step_name not in archive._get_next_steps(
                prev_step_name
            ):
                raise Exception("Invalid Step order")

            step = Step.objects.create(
                archive=archive,
                name=step_name,
                input_step_id=input_step_id,
                status=Status.WAITING,
            )

            archive.pipeline_steps.append(step.id)
            archive.save()

    def get_next_steps(self):
        with transaction.atomic():
            locked_archive = Archive.objects.select_for_update().get(pk=self.pk)

            # Determine the last step's type
            if len(locked_archive.pipeline_steps) == 0:
                if not locked_archive.last_step:
                    return []
                step_name = locked_archive.last_step.name
            else:
                step_name = Step.objects.get(pk=locked_archive.pipeline_steps[-1]).name

            # Get possible next steps
            return locked_archive._get_next_steps(step_name)

    def _get_next_steps(self, step_name):
        next_steps = pipeline.get_next_steps(step_name).copy()  # shallow

        if (
            not self.title
            or self.title == ""
            or self.title == f"{self.source} - {self.recid}"
        ) and self.state != ArchiveState.NONE:
            next_steps.append(Steps.EXTRACT_TITLE)

        if self.state == ArchiveState.AIP:
            if Steps.PUSH_TO_CTA not in next_steps:
                next_steps.append(Steps.PUSH_TO_CTA)

            source = Source.objects.all().filter(name=self.source)
            if (
                len(source) > 0
                and source[0].notification_endpoint
                and Steps.NOTIFY_SOURCE not in next_steps
            ):
                next_steps.append(Steps.NOTIFY_SOURCE)

        return next_steps


class Step(models.Model):
    """
    A single “processing” step in the archival process
    """

    id = models.AutoField(primary_key=True)
    # The archival process this step is in
    archive = models.ForeignKey(Archive, on_delete=models.PROTECT, related_name="steps")
    name = models.IntegerField(choices=Steps.choices)
    create_date = models.DateTimeField(default=timezone.now)
    start_date = models.DateTimeField(default=None, null=True)
    finish_date = models.DateTimeField(default=None, null=True)
    status = models.IntegerField(choices=Status.choices, default=Status.NOT_RUN)

    celery_task_id = models.CharField(max_length=50, null=True, default=None)
    input_data = models.TextField(null=True, default=None)
    input_step = models.ForeignKey(
        "self",
        on_delete=models.PROTECT,
        related_name="step",
        null=True,
        blank=True,
    )
    output_data = models.TextField(null=True, default=None)

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

    permissions = [
        ("can_access_all_archives"),
    ]

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


class UploadJob(models.Model):
    """
    An upload job with a unique ID and a set of associated files
    """

    id = models.AutoField(primary_key=True)
    creator = models.ForeignKey(
        User, on_delete=models.PROTECT, null=True, related_name="uploadjobs"
    )
    timestamp = models.DateTimeField(default=timezone.now)
    tmp_dir = models.CharField(max_length=1000)
    sip_dir = models.CharField(max_length=1000)
    files = models.JSONField()

    class Meta:
        ordering = ["-id"]

    def get_files(self):
        return json.loads(self.files)

    def add_file(self, local_path, sip_path):
        files = json.loads(self.files)
        files[local_path] = sip_path
        self.files = json.dumps(files)
        self.save(update_fields=["files"])

    def set_sip_dir(self, sip_dir):
        self.sip_dir = sip_dir
        self.save(update_fields=["sip_dir"])


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
