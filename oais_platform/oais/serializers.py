from django.contrib.auth.models import Group, User
from opensearch_dsl import utils
from rest_framework import serializers

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    Collection,
    Profile,
    Resource,
    Source,
    Step,
)


class ProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = Profile
        fields = [
            "cern_roles",
        ]


class ResourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Resource
        fields = [
            "id",
            "source",
            "recid",
            "invenio_id",
            "invenio_parent_id",
            "invenio_parent_url",
        ]


class SourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Source
        fields = "__all__"


class APIKeySerializer(serializers.ModelSerializer):
    source = serializers.SlugRelatedField(read_only=True, slug_field="name")

    class Meta:
        model = ApiKey
        fields = ["source", "key"]


class UserSerializer(serializers.ModelSerializer):
    permissions = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            "id",
            "username",
            "permissions",
            "first_name",
            "last_name",
        ]

    def get_permissions(self, obj):
        if type(obj) == utils.AttrDict:
            id = obj["id"]
            obj = User.objects.get(pk=id)
        return obj.get_all_permissions()


class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ["url", "name"]


class StepSerializer(serializers.ModelSerializer):
    archive = serializers.IntegerField(source="archive.id")

    class Meta:
        model = Step
        fields = [
            "id",
            "archive",
            "name",
            "start_date",
            "finish_date",
            "status",
            "celery_task_id",
            "input_data",
            "input_step",
            "output_data",
        ]


class LastStepSerializer(serializers.ModelSerializer):
    class Meta:
        model = Step
        fields = [
            "id",
            "name",
            "start_date",
            "finish_date",
            "status",
        ]


class ArchiveSerializer(serializers.ModelSerializer):
    creator = UserSerializer()
    resource = ResourceSerializer()
    last_step = LastStepSerializer(many=False, read_only=True)
    last_update = serializers.CharField(source="last_modification_timestamp")

    class Meta:
        model = Archive
        fields = [
            "id",
            "source_url",
            "recid",
            "source",
            "creator",
            "timestamp",
            "last_step",
            "last_completed_step",
            "path_to_sip",
            "next_steps",
            "manifest",
            "staged",
            "title",
            "restricted",
            "invenio_version",
            "resource",  # this points to the serialized resource
            "state",
            "last_update",
        ]

    def get_last_step(self, instance):
        last_step = instance.steps.all().order_by("-start_date")[0]
        return last_step


class CollectionSerializer(serializers.ModelSerializer):
    archives = ArchiveSerializer(many=True)
    creator = UserSerializer()

    class Meta:
        model = Collection
        fields = [
            "id",
            "title",
            "description",
            "creator",
            "timestamp",
            "last_modification_date",
            "archives",
        ]


class CollectionNameSerializer(serializers.ModelSerializer):
    class Meta:
        model = Collection
        fields = [
            "id",
            "title",
        ]


class LoginSerializer(serializers.Serializer):
    username = serializers.CharField(max_length=150)
    password = serializers.CharField(max_length=128)


class SourceRecordSerializer(serializers.Serializer):
    source = serializers.CharField(max_length=150, required=True)
    recid = serializers.CharField(max_length=128, required=True)
