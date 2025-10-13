from django.contrib import admin
from django.db.models import Count
from django.urls import reverse
from django.utils.html import format_html

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    Collection,
    HarvestBatch,
    HarvestRun,
    Profile,
    Resource,
    ScheduledHarvest,
    Source,
    Step,
    StepType,
    UploadJob,
)


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


@admin.register(UploadJob)
class UploadJobAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = ("id", "creator_name", "timestamp", "sip_dir")

    def creator_name(self, obj):
        if obj.creator:
            return obj.creator.username
        return None

    creator_name.short_description = "Creator"


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


@admin.register(ScheduledHarvest)
class ScheduledHarvestAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "source_name",
        "enabled",
        "pipeline",
        "condition_unmodified_for_days",
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
        "archive_count",
        "query_start_time",
        "query_end_time",
        "condition_unmodified_for_days",
        "created_at",
        "batch_size",
        "batch_delay_minutes",
    )

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

    def archive_count(self, obj):
        return obj.size

    archive_count.short_description = "Archives Count"


@admin.register(HarvestBatch)
class HarvestBatchAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    ordering = ["-id"]
    list_filter = ["status", "harvest_run"]

    list_display = (
        "id",
        "batch_number",
        "status",
        "harvest_run_link",
        "size",
        "completed",
        "failed",
        "archive_count",
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
