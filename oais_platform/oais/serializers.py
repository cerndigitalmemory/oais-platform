from django.contrib.auth.models import User
from django.db.models import (
    CharField,
    Count,
    DateTimeField,
    IntegerField,
    Max,
    Min,
    Value,
)
from django.db.models.functions import Coalesce
from drf_spectacular.utils import extend_schema_field
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

    @extend_schema_field(serializers.BooleanField)
    def get_is_superuser(self, obj):
        return obj.is_superuser

    @extend_schema_field(serializers.ListField(child=serializers.CharField()))
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

    @extend_schema_field(serializers.IntegerField)
    def get_archives_aip_count(self, obj):
        return obj.archives.filter(state=ArchiveState.AIP).count()

    @extend_schema_field(serializers.IntegerField)
    def get_archives_sip_count(self, obj):
        return obj.archives.filter(state=ArchiveState.SIP).count()

    @extend_schema_field(serializers.IntegerField)
    def get_archives_no_package_count(self, obj):
        return obj.archives.filter(state=ArchiveState.NONE).count()

    @extend_schema_field(serializers.DictField())
    def get_archives_summary(self, obj):
        qs = (
            obj.archives.annotate(
                step_name=Coalesce(
                    "last_step__step_type__name",
                    Value(None),
                    output_field=CharField(),
                ),
                step_status=Coalesce(
                    "last_step__status",
                    Value(None),
                    output_field=IntegerField(),
                ),
                step_ts=Coalesce(
                    "last_step__start_date",
                    Value(None),
                    output_field=DateTimeField(),
                ),
            )
            .values("step_name", "step_status")
            .annotate(
                count=Count("id"),
                min_last_update=Min("step_ts"),
                max_last_update=Max("step_ts"),
            )
        )
        summary = {}
        for row in qs:
            step = row["step_name"]
            status = str(row["step_status"])  # JSON-friendly keys

            summary.setdefault(step, {})[status] = {
                "count": row["count"],
                "min_last_update": row["min_last_update"],
                "max_last_update": row["max_last_update"],
            }

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


class SearchResultSerializer(serializers.Serializer):
    results = serializers.ListField()
    total_num_hits = serializers.IntegerField()


class SearchByIdResultSerializer(serializers.Serializer):
    result = serializers.ListField()


class ParseUrlSerializer(serializers.Serializer):
    url = serializers.URLField(required=True)


class ParseUrlResultSerializer(serializers.Serializer):
    source = serializers.CharField(max_length=150)
    recid = serializers.CharField(max_length=128)


class CallbackSerializer(serializers.Serializer):
    package_uuid = serializers.CharField(max_length=128, required=True)
    package_name = serializers.CharField(max_length=256, required=True)


class AnnounceSerializer(serializers.Serializer):
    announce_path = serializers.CharField(max_length=1024, required=True)


class BatchAnnounceSerializer(serializers.Serializer):
    batch_announce_path = serializers.CharField(max_length=1024, required=True)
    batch_tag = serializers.CharField(max_length=256, required=False)


class FileUploadSerializer(serializers.Serializer):
    file = serializers.FileField(required=True, help_text="File to upload")
    title = serializers.CharField(
        required=False, allow_blank=True, help_text="Archive title"
    )
    author = serializers.CharField(
        required=False, allow_blank=True, help_text="Author name"
    )
