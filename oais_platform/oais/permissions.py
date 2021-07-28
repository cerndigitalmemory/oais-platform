def filter_archives_by_user_perms(queryset, user):
    """Filters a queryset of archives based on the user's permissions.

    In particular, if the user does not have the "oais.can_access_all_archives"
    permission, then the queryset will be filtered to only include archives
    created by the user.
    """
    if not user.has_perm("oais.can_access_all_archives"):
        queryset = queryset.filter(creator=user)
    return queryset
