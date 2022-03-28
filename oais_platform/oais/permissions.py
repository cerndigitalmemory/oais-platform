from guardian.shortcuts import get_objects_for_user
from itertools import chain
from django.db.models import Q


def filter_archives_by_user_creator(queryset, user):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to only include archives
    created by the user.
    """
    queryset = queryset.filter(creator=user)
    return queryset


def filter_archives_public(queryset):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to include all public archives.
    """
    queryset = queryset.filter(restricted=False)
    return queryset


def filter_archives_for_user(queryset, user):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will return all the archives user has been granted access to
    but they are restricted.
    """
    if not user.has_perm("oais.can_access_all_archives"):
        private_others_queryset = get_objects_for_user(user, "oais.view_archive")
        private_owned_queryset = queryset.filter(Q(restricted=True) & Q(creator=user))
        queryset = private_others_queryset | private_owned_queryset
    return queryset


def filter_all_archives_user_has_access(queryset, user):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will return all the archives user has access to
    (Public, Private and Owned).
    """
    if not user.has_perm("oais.can_access_all_archives"):
        private_queryset = get_objects_for_user(user, "oais.view_archive")
        public_queryset = queryset.filter(restricted=False)
        owned_queryset = queryset.filter(creator=user)
        queryset = private_queryset | public_queryset | owned_queryset

    return queryset


def filter_archives_public(queryset):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to only include archives
    created by the user.
    """
    queryset = queryset.filter(restricted=False)
    return queryset


def filter_archives_for_user(queryset, user):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will return all the archives user has been granted access to
    but they are restricted.
    """
    if not user.has_perm("oais.can_access_all_archives"):
        private_others_queryset = get_objects_for_user(user, "oais.view_archive")
        private_owned_queryset = queryset.filter(Q(restricted=True) & Q(creator=user))
        queryset = sorted(
            chain(private_others_queryset, private_owned_queryset),
            key=lambda instance: instance.timestamp,
            reverse=True,
        )
    return queryset


def filter_all_archives_user_has_access(queryset, user):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will return all the archives user has access to
    (Public, Private and Owned).
    """
    if not user.has_perm("oais.can_access_all_archives"):
        private_queryset = get_objects_for_user(user, "oais.view_archive")
        public_or_owned_queryset = queryset.filter(
            Q(restricted=False) | Q(creator=user)
        )
        queryset = sorted(
            chain(private_queryset, public_or_owned_queryset),
            key=lambda instance: instance.timestamp,
            reverse=True,
        )

    return queryset


def filter_steps_by_user_perms(queryset, user):
    """Filters a queryset of steps based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to only include archives
    created by the user.
    """
    if not user.has_perm("oais.can_access_all_archives"):
        queryset = queryset.filter(archive__creator=user)
    return queryset


def filter_collections_by_user_perms(queryset, user):
    """Filters a queryset of collections based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to only include archives
    created by the user.
    """
    if not user.has_perm("oais.can_access_all_archives"):
        queryset = queryset.filter(creator=user)
    queryset = queryset.filter(internal=False)
    return queryset


def filter_jobs_by_user_perms(queryset, user):
    """Filters a queryset of collections based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to only include archives
    created by the user.
    """
    if not user.has_perm("oais.can_access_all_archives"):
        queryset = queryset.filter(creator=user)
    queryset = queryset.filter(internal=True)
    return queryset


def filter_records_by_user_perms(queryset, user):
    """Filters a queryset of records based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to only include archives
    created by the user.
    """
    if not user.has_perm("oais.can_access_all_archives"):
        queryset = queryset.filter(creator=user)
    return queryset
