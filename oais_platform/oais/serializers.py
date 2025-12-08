from django.contrib.auth.models import Group, User
from opensearch_dsl import utils
from rest_framework import serializers

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    ArchiveState,
    Collection,
    Profile,
    Resource,
    Source,
    Step,
    StepType,
)


class ProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = Profile
        fields = [
            "department",
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
        fields = ["id", "name", "longname", "enabled", "description", "how_to_get_key"]


class APIKeySerializer(serializers.ModelSerializer):
    source = SourceSerializer

    class Meta:
        model = ApiKey
        fields = ["source", "key"]


class UserSerializer(serializers.ModelSerializer):
    is_superuser = serializers.SerializerMethodField()
    permissions = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = [
            "id",
            "username",
            "permissions",
            "first_name",
            "last_name",
            "is_superuser",
        ]

    def get_is_superuser(self, obj):
        return obj.is_superuser

    def get_permissions(self, obj):
        if type(obj) is utils.AttrDict:
            id = obj["id"]
            obj = User.objects.get(pk=id)
        return obj.get_all_permissions()


class UserMinimalSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = [
            "id",
            "username",
        ]


class StepTypeMinimalSerializer(serializers.ModelSerializer):
    class Meta:
        model = StepType
        fields = [
            "id",
            "name",
            "label",
            "description",
            "enabled",
        ]


class StepSerializer(serializers.ModelSerializer):
    archive = serializers.IntegerField(source="archive.id")
    step_type = StepTypeMinimalSerializer()

    class Meta:
        model = Step
        fields = [
            "id",
            "archive",
            "step_type",
            "create_date",
            "start_date",
            "finish_date",
            "status",
            "celery_task_id",
            "input_data",
            "input_step",
            "output_data",
        ]


class LastStepSerializer(serializers.ModelSerializer):
    step_type = StepTypeMinimalSerializer()

    class Meta:
        model = Step
        fields = [
            "id",
            "step_type",
            "start_date",
            "finish_date",
            "status",
        ]


class ArchiveSerializer(serializers.ModelSerializer):
    approver = UserMinimalSerializer()
    requester = UserMinimalSerializer()
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
            "approver",
            "requester",
            "timestamp",
            "last_step",
            "last_completed_step",
            "path_to_sip",
            "manifest",
            "staged",
            "title",
            "restricted",
            "resource",  # this points to the serialized resource
            "state",
            "last_update",
        ]


class ArchiveWithDuplicatesSerializer(ArchiveSerializer):
    duplicates = serializers.SerializerMethodField()

    class Meta(ArchiveSerializer.Meta):
        fields = ArchiveSerializer.Meta.fields + ["duplicates"]

    def get_duplicates(self, obj):
        duplicates = self.context.get("duplicates").filter(resource__id=obj.resource.id)
        results = []
        for d in duplicates:
            timestamp_match = (
                obj.version_timestamp == d.version_timestamp
                and obj.version_timestamp is not None
            )
            results.append(
                {
                    "id": d.id,
                    "timestamp": d.timestamp,
                    "timestamp_match": timestamp_match,
                }
            )
        return results


class ArchiveMinimalSerializer(serializers.ModelSerializer):
    approver = UserMinimalSerializer()
    requester = UserMinimalSerializer()
    last_step = LastStepSerializer(many=False, read_only=True)
    last_update = serializers.CharField(source="last_modification_timestamp")

    class Meta:
        model = Archive
        fields = [
            "id",
            "source_url",
            "recid",
            "source",
            "approver",
            "requester",
            "timestamp",
            "last_step",
            "title",
            "state",
            "last_update",
        ]


class CollectionSerializer(serializers.ModelSerializer):
    archives_count = serializers.IntegerField(source="archives.count", read_only=True)
    creator = UserMinimalSerializer()

    archives_summary = serializers.SerializerMethodField()
    archives_sip_count = serializers.SerializerMethodField()
    archives_aip_count = serializers.SerializerMethodField()
    archives_no_package_count = serializers.SerializerMethodField()

    class Meta:
        model = Collection
        fields = [
            "id",
            "title",
            "description",
            "creator",
            "timestamp",
            "last_modification_date",
            "archives_count",
            "archives_summary",
            "archives_sip_count",
            "archives_aip_count",
            "archives_no_package_count",
        ]

    def get_archives_aip_count(self, obj):
        return obj.archives.filter(state=ArchiveState.AIP).count()

    def get_archives_sip_count(self, obj):
        return obj.archives.filter(state=ArchiveState.SIP).count()

    def get_archives_no_package_count(self, obj):
        return obj.archives.filter(state=ArchiveState.NONE).count()

    def get_archives_summary(self, obj):
        summary = {}
        for archive in obj.archives.all():
            if archive.last_step:
                step_name = archive.last_step.step_type.name
                status = archive.last_step.status
                timestamp_str = archive.last_step.start_date
            else:
                step_name, status, timestamp_str = ("None", "None", "None")
            if not step_name in summary:
                summary[step_name] = {}
            if not status in summary[step_name]:
                summary[step_name][status] = {
                    "count": 0,
                    "min_last_update": timestamp_str,
                    "max_last_update": timestamp_str,
                }
            summary[step_name][status]["count"] += 1
            if timestamp_str < summary[step_name][status]["min_last_update"]:
                summary[step_name][status]["min_last_update"] = timestamp_str
            if timestamp_str > summary[step_name][status]["max_last_update"]:
                summary[step_name][status]["max_last_update"] = timestamp_str
        return summary


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
