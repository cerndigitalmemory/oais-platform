from django.contrib import admin, messages
from django.db.models import Count
from django.urls import reverse
from django.utils.html import format_html

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    BatchStatus,
    Collection,
    HarvestBatch,
    HarvestRun,
    PersonalAccessToken,
    Profile,
    Resource,
    ScheduledHarvest,
    Source,
    Step,
    StepType,
)
from oais_platform.oais.tasks.scheduled_harvest import batch_harvest


class NullToNotRequiredMixin:
    """Override form, set nullable field as not required."""

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        for field_name, field in form.base_fields.items():
            model_field = self.model._meta.get_field(field_name)
            if model_field.null:
                field.required = False
        return form


# Register your models here.


@admin.register(Archive)
class ArchiveAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "timestamp",
        "source",
        "recid",
        "state",
        "last_completed_step_link",
        "last_step_link",
        "staged",
        "title",
        "resource_link",
    )

    raw_id_fields = ["resource", "last_step", "last_completed_step"]

    def last_step_link(self, obj):
        related_obj = obj.last_step
        if related_obj:
            url = reverse("admin:oais_step_change", args=[related_obj.id])
            return format_html('<a href="{}">{}</a>', url, related_obj)
        return None

    last_step_link.short_description = "Last Step"

    def last_completed_step_link(self, obj):
        related_obj = obj.last_completed_step
        if related_obj:
            url = reverse("admin:oais_step_change", args=[related_obj.id])
            return format_html('<a href="{}">{}</a>', url, related_obj)
        return None

    last_completed_step_link.short_description = "Last Completed Step"

    def resource_link(self, obj):
        related_obj = obj.resource
        if related_obj:
            url = reverse("admin:oais_resource_change", args=[related_obj.id])
            return format_html('<a href="{}">{}</a>', url, related_obj)
        return None

    resource_link.short_description = "Resource"

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        if "pipeline_steps" in form.base_fields:
            # Gives error for empty list so escape the validation
            field = form.base_fields["pipeline_steps"]
            field.required = False
        return form


@admin.register(Step)
class StepAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "archive_link",
        "step_type_name",
        "status",
        "start_date",
        "finish_date",
    )

    raw_id_fields = [
        "archive",
        "initiated_by_harvest_batch",
        "input_step",
        "initiated_by_user",
    ]

    def archive_link(self, obj):
        related_obj = obj.archive
        if related_obj:
            url = reverse("admin:oais_archive_change", args=[related_obj.id])
            return format_html('<a href="{}">{}</a>', url, related_obj)
        return None

    archive_link.short_description = "Archive"

    def step_type_name(self, obj):
        if obj.step_type:
            return obj.step_type.name
        return None

    step_type_name.short_description = "Step Type"


@admin.register(StepType)
class StepTypeAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "label",
        "description",
        "enabled",
        "task_name",
        "failed_count",
        "failed_blocking_limit",
        "has_sip",
        "has_aip",
        "automatic_next_step_name",
    )

    def automatic_next_step_name(self, obj):
        if obj.automatic_next_step:
            return obj.automatic_next_step.name
        return None

    automatic_next_step_name.short_description = "Automatic Next Step"


@admin.register(Resource)
class ResourceAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = ("id", "source", "recid")


@admin.register(Collection)
class CollectionAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "timestamp",
        "title",
        "description",
        "internal",
        "archive_count",
    )

    raw_id_fields = ["archives"]

    def archive_count(self, obj):
        return obj.archive_count

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        queryset = queryset.annotate(archive_count=Count("archives"))
        return queryset


@admin.register(Profile)
class ProfileAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = ["user_name", "department", "system"]
    list_filter = ["department", "system"]

    def user_name(self, obj):
        if obj.user:
            return obj.user.username
        return None

    user_name.short_description = "Username"


@admin.register(Source)
class SourceAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "longname",
        "timestamp",
        "api_url",
        "enabled",
        "classname",
        "has_restricted_records",
        "has_public_records",
        "how_to_get_key",
        "description",
        "notification_enabled",
        "notification_endpoint",
    )


@admin.register(ApiKey)
class APIKeyAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = ["user_name", "source_name", "_key"]

    def user_name(self, obj):
        if obj.user:
            return obj.user.username
        return None

    user_name.short_description = "Username"

    def source_name(self, obj):
        if obj.source:
            return obj.source.name
        return None

    source_name.short_description = "Source"


@admin.register(PersonalAccessToken)
class PersonalAccessTokenAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    # Can only be created from the command line
    def has_add_permission(self, request, obj=None):
        return False

    list_display = [
        "name",
        "user_name",
        "created_at",
        "last_used_at",
        "expires_at",
        "revoked",
    ]

    def user_name(self, obj):
        if obj.user:
            return obj.user.username
        return None

    user_name.short_description = "Username"


@admin.register(ScheduledHarvest)
class ScheduledHarvestAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "source_name",
        "enabled",
        "pipeline",
        "filter_type",
        "grace_period_days",
        "scheduling_task",
        "batch_size",
        "batch_delay_minutes",
    )

    def source_name(self, obj):
        if obj.source:
            return obj.source.name
        return None

    source_name.short_description = "Source"


@admin.register(HarvestRun)
class HarvestRunAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "source_name",
        "scheduled_harvest_link",
        "collection_link",
        "pipeline",
        "size",
        "archive_count",
        "skipped_count",
        "query_start_time",
        "query_end_time",
        "filter_type",
        "grace_period_days",
        "created_at",
        "batch_size",
        "batch_delay_minutes",
    )

    raw_id_fields = ["collection"]

    def source_name(self, obj):
        if obj.source:
            return obj.source.name
        return None

    source_name.short_description = "Source"

    def collection_link(self, obj):
        related_obj = obj.collection
        if related_obj:
            url = reverse("admin:oais_collection_change", args=[related_obj.id])
            return format_html('<a href="{}">{}</a>', url, related_obj)
        return None

    collection_link.short_description = "Collection"

    def scheduled_harvest_link(self, obj):
        related_obj = obj.scheduled_harvest
        if related_obj:
            url = reverse("admin:oais_scheduledharvest_change", args=[related_obj.id])
            return format_html('<a href="{}">{}</a>', url, related_obj)
        return None

    scheduled_harvest_link.short_description = "Scheduled Harvest"

    def size(self, obj):
        return obj.size

    size.short_description = "Size"

    def archive_count(self, obj):
        return obj.archive_count

    archive_count.short_description = "Archives Count"


@admin.register(HarvestBatch)
class HarvestBatchAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    ordering = ["-id"]
    list_filter = ["status", "harvest_run"]
    actions = ["continue_batch"]

    list_display = (
        "id",
        "batch_number",
        "status",
        "harvest_run_link",
        "size",
        "completed",
        "failed",
        "archive_count",
        "skipped_count",
    )

    def harvest_run_link(self, obj):
        related_obj = obj.harvest_run
        if related_obj:
            url = reverse("admin:oais_harvestrun_change", args=[related_obj.id])
            return format_html('<a href="{}">{}</a>', url, related_obj)
        return None

    harvest_run_link.short_description = "Harvest Run"

    def archive_count(self, obj):
        if obj.archives:
            return obj.archives.count()
        return 0

    archive_count.short_description = "Archives Count"

    def continue_batch(modeladmin, request, queryset):
        """
        In case the batch processing stopped (blocked or failed)
        this action should be triggered for the first PENDING batch
        """
        if queryset.count() != 1:
            modeladmin.message_user(
                request,
                "Please select exactly one PENDING item for this action.",
                level=messages.ERROR,
            )
            return

        item = queryset.first()
        if item.status != BatchStatus.PENDING:
            modeladmin.message_user(
                request,
                f"Selected item has status '{item.status}'. Continue action is limited to PENDING batches.",
                level=messages.ERROR,
            )
            return

        try:
            batch_harvest.delay(item.id)
            modeladmin.message_user(
                request,
                f"Successfully sent Batch Harvest task to Celery: batch {item.id}",
                level=messages.SUCCESS,
            )
        except Exception as e:
            modeladmin.message_user(
                request,
                f"Failed to trigger batch harvest task for batch {item.id}: {str(e)}",
                level=messages.ERROR,
            )

    continue_batch.short_description = "Continue Batch Harvest"
