from django.db.models import Q
from guardian.shortcuts import get_objects_for_user, get_perms
from rest_framework import permissions

from oais_platform.oais.models import Archive


class UserPermission(permissions.BasePermission):
    def has_permission(self, request, view):
        if view.action == "list":
            return request.user.is_superuser

        return request.user.is_authenticated

    def has_object_permission(self, request, view, obj):
        if request.user.is_superuser:
            return True
        if obj.id == request.user.id:
            return True
        return False


class ArchivePermission(permissions.BasePermission):
    def has_permission(self, request, view):
        user = request.user
        if user.is_superuser:
            return True
        if view.action in ["archives_unstage"]:
            if self._can_view_archive_list(user, request.data["archives"]):
                return self._can_approve_archive(user)
            else:
                return False
        elif view.action in ["archive_action_intersection"]:
            return self._can_view_archive_list(user, request.data["archives"])
        elif view.action == "list" and request.GET.get("access", "all") == "public":
            return True
        else:
            return user.is_authenticated

    def has_object_permission(self, request, view, obj):
        user = request.user
        if user.is_superuser:
            return True
        if view.action in ["archive_save_manifest"]:  # Actions that need edit right
            return self._can_edit_archive(user, obj)
        elif view.action in ["archive_unstage"]:  # Actions that need approve right
            return self._can_approve_archive(user, obj)
        elif view.action in [
            "archive_run_pipeline"
        ]:  # Actions that need execute_steps right
            return self._can_execute_steps(user, obj)
        else:
            return self._can_view_archive(request.user, obj)

    def _can_view_archive(self, user, archive):
        if user.is_superuser or user.has_perm("oais.view_archive_all"):
            return True
        elif "oais.view_arhive" in get_perms(user, archive):
            return True
        elif archive.requester == user or archive.approver == user:
            return True
        return False

    def _can_view_archive_list(self, user, archives):
        for archive in archives:
            if type(archive) is int:
                archive = Archive.objects.get(id=archive)
            elif type(archive) is dict:
                archive = Archive.objects.get(id=archive["id"])
            if not self._can_view_archive(user, archive):
                return False
        return True

    def _can_edit_archive(self, user, archive):
        if not self._can_view_archive(user, archive):
            return False
        if (
            user.has_perm("oais.can_edit_all")
            or archive.approver == user
            or archive.requester == user
        ):
            return True
        return False

    def _can_approve_archive(self, user, archive=None):
        if archive and not self._can_view_archive(user, archive):
            return False
        if user.has_perm("oais.can_approve_all"):
            return True
        return False

    def _can_execute_steps(self, user, archive):
        if not self._can_view_archive(user, archive):
            return False
        if user.has_perm("oais.can_execute_step"):
            return True
        return False


class StepPermission(permissions.BasePermission):
    archive_perms = ArchivePermission()

    def has_permission(self, request, view):
        return request.user.is_authenticated

    def has_object_permission(self, request, view, obj):
        return self.archive_perms._can_view_archive(request.user, obj.archive)


class TagPermission(permissions.BasePermission):
    archive_perms = ArchivePermission()

    def has_permission(self, request, view):
        return request.user.is_authenticated

    def has_object_permission(self, request, view, obj):
        user = request.user
        if user.is_superuser:
            return True
        if view.action in ["create_tag", "add_arch", "remove_arch"]:
            if not user.id == obj.creator.id:
                return False
            if request.data["archives"]:
                return self.archive_perms._can_view_archive_list(
                    user, request.data["archives"]
                )
            return True
        if view.action in ["edit_tag", "delete_tag"]:
            return user.id == obj.creator.id
        return self.archive_perms._can_view_archive_list(
            request.user, obj.archives.all()
        )


class SuperUserPermission(permissions.BasePermission):
    def has_permission(self, request, view):
        return request.user.is_superuser

    def has_object_permission(self, request, view, obj):
        return request.user.is_superuser


def filter_archives(queryset, user=None, visibility="all"):
    match visibility:
        case "all":
            if user.is_superuser or user.has_perm("oais.view_archive_all"):
                return queryset
            permission_granted_queryset = get_objects_for_user(
                user, "oais.view_archive"
            )
            return (
                queryset.filter(
                    Q(approver=user) | Q(requester=user) | Q(restricted=False)
                )
                | permission_granted_queryset
            )
        case "owned":
            return queryset.filter(Q(approver=user) | Q(requester=user))
        case "public":
            return queryset.filter(restricted=False)


def filter_collections(queryset, user, internal=None):
    if not user.has_perm("oais.view_archive_all"):
        queryset = queryset.filter(creator=user)
    if internal is not None:
        queryset = queryset.filter(internal=internal)
    return queryset
