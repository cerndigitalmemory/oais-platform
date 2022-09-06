from re import S

from django.contrib.auth.models import Group, User
from oais_platform.oais.models import Archive, Collection, Profile, Step, Resource
from opensearch_dsl import utils
from rest_framework import serializers
from rest_framework.fields import IntegerField


class ProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = Profile
        fields = [
            "indico_api_key",
            "codimd_api_key",
            "sso_comp_token",
            "gitlab_api_key",
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


class UserSerializer(serializers.ModelSerializer):
    permissions = serializers.SerializerMethodField()

    # Serialize the additional profile values
    profile = ProfileSerializer(required=True)

    class Meta:
        model = User
        fields = [
            "id",
            "username",
            "permissions",
            "first_name",
            "last_name",
            "profile",  # this points to the serialized profile
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


class ArchiveSerializer(serializers.ModelSerializer):
    creator = UserSerializer()
    resource = ResourceSerializer()

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
            "path_to_sip",
            "next_steps",
            "manifest",
            "staged",
            "title",
            "restricted",
            "invenio_version",
            "resource",  # this points to the serialized resource
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


class LoginSerializer(serializers.Serializer):
    username = serializers.CharField(max_length=150)
    password = serializers.CharField(max_length=128)


class SourceRecordSerializer(serializers.Serializer):
    source = serializers.CharField(max_length=150, required=True)
    recid = serializers.CharField(max_length=128, required=True)
