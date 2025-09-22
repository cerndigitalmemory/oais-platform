from django.db.models import Exists, OuterRef

from oais_platform.oais.models import Archive, Status, Step


def count_archives_by_steps(category):
    """
    Returns count of Archives based on included and excluded completed steps.

    :param category: A dictionary containing values to filter archives by
    """
    include_steps = category.get("included", [])
    exclude_steps = category.get("excluded", [])

    archives = Archive.objects.all()

    if category.get("state"):
        archives = archives.filter(state=category["state"])

    if category.get("staged"):
        archives = archives.filter(staged=category["staged"])

    for step_name in include_steps:
        archives = archives.filter(
            Exists(
                Step.objects.filter(
                    archive=OuterRef("pk"),
                    step_type__name=step_name,
                    status=Status.COMPLETED,
                )
            )
        )

    for step_name in exclude_steps:
        archives = archives.filter(
            ~Exists(
                Step.objects.filter(
                    archive=OuterRef("pk"),
                    step_type__name=step_name,
                    status=Status.COMPLETED,
                )
            )
        )

    return archives.distinct().count()


def count_excluded_archives(statistics):
    """
    Returns the count of Archives that do not belong to any of the predefined categories.

    :param statistics: A dictionary containing the counts for each category.
    """
    return Archive.objects.all().count() - sum(statistics.values())
