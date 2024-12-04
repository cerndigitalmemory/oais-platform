from django.contrib import admin
from django.db.models import Count
from django.urls import reverse
from django.utils.html import format_html

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    Collection,
    Profile,
    Resource,
    Source,
    Step,
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


@admin.register(Step)
class StepAdmin(NullToNotRequiredMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "archive_link",
        "name",
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
    list_display = ["user_name"]

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
