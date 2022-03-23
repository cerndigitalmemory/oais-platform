from guardian.shortcuts import get_objects_for_user


def filter_archives_by_user_perms(queryset, user):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to only include archives
    created by the user.
    """
    if not user.has_perm("oais.can_access_all_archives"):
        queryset = queryset.filter(creator=user)
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
    if not user.has_perm("oais.can_access_all_archives"):
        queryset = get_objects_for_user(user, "oais.view_archive")
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
