from django.contrib.auth.models import User
from django.db.models import (
    Avg,
    Case,
    Count,
    DurationField,
    ExpressionWrapper,
    F,
    IntegerField,
    OuterRef,
    Subquery,
    When,
)
from django.db.models.functions import Coalesce
from django.utils import timezone
from drf_spectacular.utils import extend_schema_field
from opensearch_dsl import utils
from rest_framework import serializers

from oais_platform.oais.enums import Status, StepName
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
from oais_platform.oais.statistics import avg_duration_per_day


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
    input_data = serializers.JSONField(source="input_data_json")
    output_data = serializers.JSONField(source="output_data_json")

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
            "removable",
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
        duplicates_ctx = self.context.get("duplicates")
        if duplicates_ctx is not None:
            duplicates = duplicates_ctx.filter(resource__id=obj.resource.id)
        else:
            duplicates = Archive.objects.filter(resource__id=obj.resource.id).exclude(
                id=obj.id
            )

        return [
            {
                "id": d.id,
                "timestamp": d.timestamp,
                "timestamp_match": (
                    obj.version_timestamp == d.version_timestamp
                    and obj.version_timestamp is not None
                ),
            }
            for d in duplicates
        ]


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


class CollectionMinimalSerializer(serializers.ModelSerializer):
    creator = UserMinimalSerializer()
    archives_count = serializers.IntegerField(source="archives.count", read_only=True)

    class Meta:
        model = Collection
        fields = [
            "id",
            "title",
            "description",
            "creator",
            "timestamp",
            "last_modification_date",
            "internal",
            "archives_count",
        ]


class CollectionSerializer(serializers.ModelSerializer):
    archives_count = serializers.IntegerField(source="archives.count", read_only=True)
    creator = UserMinimalSerializer()

    archives_summary = serializers.SerializerMethodField()
    archives_sip_count = serializers.SerializerMethodField()
    archives_aip_count = serializers.SerializerMethodField()
    archives_no_package_count = serializers.SerializerMethodField()
    archives_failure_summary = serializers.SerializerMethodField()
    execution_summary = serializers.SerializerMethodField()

    class Meta:
        model = Collection
        fields = [
            "id",
            "title",
            "description",
            "creator",
            "timestamp",
            "last_modification_date",
            "internal",
            "archives_count",
            "archives_summary",
            "archives_sip_count",
            "archives_aip_count",
            "archives_no_package_count",
            "archives_failure_summary",
            "execution_summary",
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

    def _get_latest_step_subquery(self):
        return (
            Step.objects.filter(
                archive=OuterRef("archive"), step_type=OuterRef("step_type")
            )
            .order_by("-start_date", "-create_date")
            .values("id")[:1]
        )

    STEP_ORDER_CASE = Case(
        When(step_type__has_sip=True, then=0),
        When(step_type__name=StepName.VALIDATION, then=1),
        When(step_type__name=StepName.EXTRACT_TITLE, then=2),
        When(step_type__name=StepName.ARCHIVE, then=3),
        When(step_type__name=StepName.NOTIFY_SOURCE, then=4),
        When(step_type__name=StepName.PUSH_TO_CTA, then=5),
        default=99,
        output_field=IntegerField(),
    )

    @extend_schema_field(serializers.DictField())
    def get_archives_summary(self, obj):
        latest_step_subquery = self._get_latest_step_subquery()
        qs = (
            Step.objects.filter(
                archive__in=obj.archives.all(), id=Subquery(latest_step_subquery)
            )
            .annotate(
                step_name=F("step_type__name"),
                step_status=F("status"),
                duration=ExpressionWrapper(
                    Coalesce(
                        F("finish_date") - F("start_date"),
                        timezone.now() - F("start_date"),
                    ),
                    output_field=DurationField(),
                ),
            )
            .values("step_name", "step_status")
            .annotate(
                count=Count("id"),
                avg_duration=Avg("duration"),
                order_index=self.STEP_ORDER_CASE,
            )
            .order_by("order_index")
        )

        summary = {}
        for row in qs:
            step = row["step_name"]
            status = str(row["step_status"])  # JSON-friendly keys

            summary.setdefault(step, {})[status] = {
                "count": row["count"],
                "avg_duration": (
                    float(f"{row['avg_duration'].total_seconds():.2f}")
                    if row["avg_duration"]
                    else None
                ),
            }

        return summary

    @extend_schema_field(serializers.DictField())
    def get_archives_failure_summary(self, obj):
        latest_step_subquery = self._get_latest_step_subquery()
        qs = (
            Step.objects.filter(
                archive__in=obj.archives.all(),
                id=Subquery(latest_step_subquery),
                status=Status.FAILED,
            )
            .values("step_type__name", "failure_type")
            .annotate(
                count=Count("id"),
                order_index=self.STEP_ORDER_CASE,
            )
            .order_by("order_index")
        )

        summary = {}
        for row in qs:
            step_name = row["step_type__name"]
            failure_type = row["failure_type"] or "Unknown"

            summary.setdefault(step_name, []).append(
                {
                    "failure_type": failure_type,
                    "count": row["count"],
                }
            )

        return summary

    @extend_schema_field(serializers.DictField())
    def get_execution_summary(self, obj):
        summary = {}
        step_names = [StepName.ARCHIVE, StepName.PUSH_TO_CTA]
        for step_name in step_names:
            summary[step_name] = avg_duration_per_day(
                collection_id=obj.id, step_name=step_name
            )
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


class FileUploadResultSerializer(serializers.Serializer):
    archive = serializers.IntegerField(
        help_text="ID of the created archive", required=False
    )
    status = serializers.IntegerField(help_text="Status")
    msg = serializers.CharField(help_text="Message", required=False, allow_blank=True)


class StatisticsSerializer(serializers.Serializer):
    harvested_count = serializers.IntegerField(help_text="Total number of SIPs")
    preserved_count = serializers.IntegerField(help_text="Total number of AIPs")
    pushed_to_tape_count = serializers.IntegerField(
        help_text="Number of archives successfully pushed to CTA"
    )
    pushed_to_registry_count = serializers.IntegerField(
        help_text="Number of archives successfully pushed to registry"
    )


class StepStatisticsSerializer(serializers.Serializer):
    staged_count = serializers.IntegerField(
        help_text="Number of staged archives (not yet harvested)"
    )
    harvested_count = serializers.IntegerField(help_text="Number of SIPs")
    harvested_preserved_count = serializers.IntegerField(help_text="Number of AIPs")
    harvested_preserved_tape_count = serializers.IntegerField(
        help_text="Number of AIP archives pushed to CTA only"
    )
    harvested_preserved_registry_count = serializers.IntegerField(
        help_text="Number of AIP archives pushed to registry only"
    )
    harvested_preserved_tape_registry_count = serializers.IntegerField(
        help_text="Number of AIP archives pushed to both CTA and registry"
    )
    others_count = serializers.IntegerField(
        help_text="Number of archives not matching any of the above categories"
    )


class ConfigurationSerializer(serializers.Serializer):
    max_file_size = serializers.IntegerField(
        help_text="Maximum allowed file size for uploads (in bytes)"
    )
    max_step_filter_combinations = serializers.IntegerField(
        help_text="Maximum allowed boolean combine groups in step filters"
    )


class LogoutSerializer(serializers.Serializer):
    status = serializers.CharField(help_text="Indicates if logout was successful")
