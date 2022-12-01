import json

from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from oais_platform.settings import INVENIO_SERVER_URL
from django.contrib.postgres.fields import ArrayField

from . import pipeline


class Profile(models.Model):
    # Each profile is linked to a user and identified by the same PK
    #  and it's used to save additional per-user values
    #  (e.g. configuration, preferences, API tokens)
    #  accessible as user.profile.VALUE
    user = models.OneToOneField(User, primary_key=True, on_delete=models.CASCADE)
    indico_api_key = models.TextField(max_length=500, blank=True)
    codimd_api_key = models.TextField(max_length=500, blank=True)
    sso_comp_token = models.TextField(max_length=500, blank=True)
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


class Status(models.IntegerChoices):
    NOT_RUN = 1
    IN_PROGRESS = 2
    FAILED = 3
    COMPLETED = 4
    WAITING_APPROVAL = 5
    REJECTED = 6
    WAITING = 7


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
    manifest = models.JSONField(default=None, null=True)
    staged = models.BooleanField(default=False)
    title = models.CharField(max_length=255, default="")
    restricted = models.BooleanField(default=True)
    # A number we'll increment every time we need to publish a new version on InvenioRDM
    invenio_version = models.IntegerField(default=0)
    # Resource attached to the archive
    resource = models.ForeignKey("Resource", null=True, on_delete=models.CASCADE)

    class Meta:
        ordering = ["-id"]
        permissions = (("grant_view_right", "Grant view right"),
                       ("can_unstage", "Can unstage a record and start the pipeline"))

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

        return self.next_steps

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

    def save(self, *args, **kwargs):

        # It is only executed on the object creation
        if not self.pk:
            # This code only happens if the objects is
            # not in the database yet. Otherwise it would
            # have pk

            # Look to see if there is a resource with the same source+recid
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

        # Normal logic of the save method
        super(Archive, self).save(*args, **kwargs)


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
        User, on_delete=models.PROTECT, null=True, related_name="uploadjobs")
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
