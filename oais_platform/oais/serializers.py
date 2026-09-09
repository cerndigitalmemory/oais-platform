from django.contrib.auth.models import User
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
            "is_staff",
        ]

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
            "failure_type",
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
            "pipeline_steps",
        ]


class ArchiveWithDuplicatesSerializer(ArchiveSerializer):
    duplicates = serializers.SerializerMethodField()

    class Meta(ArchiveSerializer.Meta):
        fields = ArchiveSerializer.Meta.fields + ["duplicates"]

    @extend_schema_field(serializers.ListField(child=serializers.DictField()))
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
    creator = UserMinimalSerializer()
    archives_count = serializers.IntegerField(source="archives.count", read_only=True)
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
            "internal",
            "archives_count",
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


class StepStatusStatisticsSerializer(serializers.Serializer):
    step = serializers.CharField(help_text="Step name")
    status = serializers.CharField(help_text="Step status")
    count = serializers.IntegerField(
        help_text="Number of steps with this step/status combination"
    )


class StepDurationStatisticsSerializer(serializers.Serializer):
    step = serializers.CharField(help_text="Step name")
    avg_duration = serializers.FloatField(
        help_text="Average duration (seconds) of currently in-progress steps"
    )


class ScheduledHarvestDetailSerializer(serializers.Serializer):
    name = serializers.CharField(help_text="Name of the ScheduledHarvest")
    grace_period_days = serializers.IntegerField(
        allow_null=True,
        help_text="Grace period in days used by the most recent HarvestRun",
    )
    last_run_date = serializers.DateTimeField(
        allow_null=True, help_text="Creation time of the most recent HarvestRun"
    )
    scope = serializers.CharField(
        help_text="'Full' or 'Partial' depending on extra_query"
    )
    archive_count = serializers.IntegerField(
        help_text="Number of Archives linked to this ScheduledHarvest"
    )


class HarvestedSourcesStatisticsSerializer(serializers.Serializer):
    source = serializers.CharField(help_text="Display name of the source")
    total_harvested = serializers.IntegerField(
        help_text="Count of unique preserved records (recid) harvested for this source"
    )
    latest_harvested = serializers.DateTimeField(
        allow_null=True,
        help_text="Most recent time a HARVEST step completed for this source",
    )
    scheduled_harvests = ScheduledHarvestDetailSerializer(
        many=True,
        help_text="List of ScheduledHarvest configured for this source, if any",
    )


class StepFailureStatisticsSerializer(serializers.Serializer):
    step = serializers.CharField(help_text="Step name")
    failure_type = serializers.CharField(help_text="Failure type")
    count = serializers.IntegerField(
        help_text="Number of failed steps with this step/failure type combination"
    )


class ConfigurationSerializer(serializers.Serializer):
    max_file_size = serializers.IntegerField(
        help_text="Maximum allowed file size for uploads (in bytes)"
    )
    max_step_filter_conditions = serializers.IntegerField(
        help_text="Maximum allowed number of step conditions in step filters"
    )


class LogoutSerializer(serializers.Serializer):
    status = serializers.CharField(help_text="Indicates if logout was successful")
    logout_url = serializers.URLField(
        required=False,
        allow_blank=True,
        help_text="URL to redirect to for logout (if applicable)",
    )
    requires_redirect = serializers.BooleanField()


class MessageSerializer(serializers.Serializer):
    msg = serializers.CharField(help_text="Human readable result of the operation")


class OperationResultSerializer(serializers.Serializer):
    status = serializers.IntegerField(help_text="0 on success, 1 on failure")
    errormsg = serializers.CharField(
        allow_null=True, help_text="Error description, null on success"
    )


class HarvestRecidsResultSerializer(OperationResultSerializer):
    collection_id = serializers.IntegerField(
        help_text="ID of the Collection grouping the created Archives"
    )


class UserApiKeySerializer(serializers.Serializer):
    source_id = serializers.IntegerField(help_text="ID of the Source")
    source = serializers.CharField(help_text="Long name of the Source")
    how_to = serializers.CharField(
        allow_null=True, help_text="Instructions to obtain an API key for this Source"
    )
    key = serializers.CharField(
        allow_null=True, help_text="API key set by the User, null if not set"
    )


class UserWithApiKeysSerializer(UserSerializer):
    api_key = UserApiKeySerializer(
        many=True,
        read_only=True,
        help_text="API key configuration for every Source requiring one",
    )

    class Meta(UserSerializer.Meta):
        fields = UserSerializer.Meta.fields + ["api_key"]


class UserApiKeyUpdateSerializer(serializers.Serializer):
    source = serializers.IntegerField(help_text="ID of the Source")
    key = serializers.CharField(
        allow_blank=True,
        allow_null=True,
        help_text="API key to store. An empty value deletes the stored key",
    )


class StagedRecordSerializer(serializers.Serializer):
    recid = serializers.CharField(help_text="Record ID on the upstream source")
    source = serializers.CharField(help_text="Name of the upstream source")
    source_url = serializers.CharField(help_text="URL of the record on the source")
    title = serializers.CharField(help_text="Title of the record")
    file_size = serializers.IntegerField(
        required=False, help_text="Size of the record files, in bytes"
    )
    updated = serializers.DateTimeField(
        required=False,
        allow_null=True,
        help_text="Last modification timestamp on the source",
    )


class StageRecordsSerializer(serializers.Serializer):
    records = StagedRecordSerializer(many=True)


class SourceRecordsSerializer(serializers.Serializer):
    records = SourceRecordSerializer(many=True)


class ArchiveDuplicateSerializer(serializers.Serializer):
    id = serializers.IntegerField(help_text="ID of the duplicate Archive")
    timestamp = serializers.DateTimeField(
        help_text="Creation timestamp of the duplicate Archive"
    )
    timestamp_match = serializers.BooleanField(
        help_text="Whether the duplicate was harvested from the same source version"
    )


class RecordWithDuplicatesSerializer(serializers.Serializer):
    recid = serializers.CharField(help_text="Record ID on the upstream source")
    source = serializers.CharField(help_text="Name of the upstream source")
    updated = serializers.DateTimeField(
        required=False,
        allow_null=True,
        help_text="Last modification timestamp on the source",
    )
    duplicates = ArchiveDuplicateSerializer(
        many=True, read_only=True, help_text="Archives already created for this record"
    )


class DuplicateCheckSerializer(serializers.Serializer):
    records = RecordWithDuplicatesSerializer(
        many=True, help_text="Records to look for in the already created Archives"
    )


class ArchiveIdsSerializer(serializers.Serializer):
    archives = serializers.ListField(
        child=serializers.IntegerField(),
        allow_empty=False,
        help_text="IDs of the Archives",
    )


class ArchiveIdListSerializer(serializers.Serializer):
    ids = serializers.ListField(
        child=serializers.IntegerField(), help_text="IDs of the matching Archives"
    )


class ArchiveReferenceSerializer(serializers.Serializer):
    id = serializers.IntegerField(help_text="ID of the Archive")


class ArchiveUnstageSerializer(serializers.Serializer):
    archives = ArchiveReferenceSerializer(
        many=True, help_text="Archives to move out of the staging area"
    )
    job_title = serializers.CharField(
        required=False,
        allow_blank=True,
        help_text="Title of the Tag grouping the unstaged Archives. "
        "Defaults to 'Job <current date and time>'",
    )


class ArchiveActionsSerializer(serializers.Serializer):
    all_last_step_failed = serializers.BooleanField(
        help_text="Whether the last Step of every passed Archive has failed"
    )
    can_continue = serializers.BooleanField(
        help_text="Whether the pipeline of every passed Archive can be continued"
    )


STEP_FILTERS_HELP_TEXT = (
    "Step conditions, optionally nested in boolean groups, e.g. "
    '{"and": [{...}, {"or": [{...}, {...}]}]}. A single condition matches on '
    "the keys `name` (StepType name), `status` (Step status) and "
    "`failure_type`, and can be tuned with the boolean modifiers `exclude`, "
    "`last_step`, `in_pipeline` and `latest`."
)


class ArchiveFiltersSerializer(serializers.Serializer):
    state = serializers.ChoiceField(
        choices=ArchiveState.choices, required=False, help_text="State of the Archive"
    )
    source = serializers.CharField(required=False, help_text="Name of the source")
    tag = serializers.IntegerField(
        required=False, help_text="ID of a Tag the Archive must belong to"
    )
    exclude_tag = serializers.IntegerField(
        required=False, help_text="ID of a Tag the Archive must not belong to"
    )
    query = serializers.CharField(
        required=False, help_text="Free text matched against title and record ID"
    )
    step_filters = serializers.DictField(
        required=False, help_text=STEP_FILTERS_HELP_TEXT
    )


class ArchiveFilterRequestSerializer(serializers.Serializer):
    filters = ArchiveFiltersSerializer()


class PipelineRunSerializer(serializers.Serializer):
    run_type = serializers.ChoiceField(
        choices=["run", "retry", "continue"],
        required=False,
        default="run",
        help_text="`run` creates a new pipeline, `retry` re-runs the last Step, "
        "`continue` resumes the existing pipeline",
    )
    pipeline_steps = serializers.ListField(
        child=serializers.CharField(),
        required=False,
        help_text="StepType names to run, in order. Required for `run`",
    )


class BulkPipelineRunSerializer(PipelineRunSerializer):
    archive_ids = serializers.ListField(
        child=serializers.IntegerField(),
        allow_empty=False,
        help_text="IDs of the Archives to run the pipeline for",
    )


class TagCreateSerializer(serializers.Serializer):
    title = serializers.CharField(help_text="Title of the Tag")
    description = serializers.CharField(
        allow_blank=True, allow_null=True, help_text="Description of the Tag"
    )
    archives = serializers.ListField(
        child=serializers.IntegerField(),
        allow_null=True,
        help_text="IDs of the Archives to tag",
    )


class TagEditSerializer(serializers.Serializer):
    title = serializers.CharField(help_text="New title of the Tag")
    description = serializers.CharField(
        allow_blank=True, allow_null=True, help_text="New description of the Tag"
    )


class CollectionNameListSerializer(serializers.Serializer):
    result = CollectionNameSerializer(many=True, read_only=True)


class CollectionSummarySerializer(serializers.Serializer):
    summary = serializers.DictField(
        help_text=(
            "Aggregated counters, keyed by StepType name. The shape depends on "
            "the requested summary `type`: `step` maps each step status to a "
            "count and an average duration, `failure` and `warning` list the "
            "failure types with their count, `execution` returns the average "
            "duration per day."
        )
    )
