from django.contrib import admin
from django.db.models import Count
from django.urls import reverse
from django.utils.html import format_html

from oais_platform.oais.models import (
    Archive,
    Collection,
    Profile,
    Resource,
    Step,
    UploadJob,
)

# Register your models here.


@admin.register(Archive)
class ArchiveAdmin(admin.ModelAdmin):
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
class StepAdmin(admin.ModelAdmin):
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
class ResourceAdmin(admin.ModelAdmin):
    list_display = ("id", "source", "recid")


@admin.register(Collection)
class CollectionAdmin(admin.ModelAdmin):
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
class ProfileAdmin(admin.ModelAdmin):
    list_display = ["user_name"]

    def user_name(self, obj):
        if obj.user:
            return obj.user.username
        return None

    user_name.short_description = "Username"


@admin.register(UploadJob)
class UploadJobAdmin(admin.ModelAdmin):
    list_display = ("id", "creator_name", "timestamp", "sip_dir")

    def creator_name(self, obj):
        if obj.creator:
            return obj.creator.username
        return None

    creator_name.short_description = "Creator"
