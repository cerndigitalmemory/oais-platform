from django.db.models import Exists, OuterRef

from oais_platform.oais.models import Archive, Status, Step


def count_archives_by_steps(include_steps=None, exclude_steps=None):
    """
    Returns count of Archives based on included and excluded completed steps.

    :param include_steps: A list or tuple of Steps.name to include (must be completed).
    :param exclude_steps: A list or tuple of Steps.name to exclude if completed.
    """
    include_steps = include_steps or []
    exclude_steps = exclude_steps or []

    archives = Archive.objects.all()

    for step_name in include_steps:
        archives = archives.filter(
            Exists(
                Step.objects.filter(
                    archive=OuterRef("pk"),
                    name=step_name,
                    status=Status.COMPLETED,
                )
            )
        )

    for step_name in exclude_steps:
        archives = archives.filter(
            ~Exists(
                Step.objects.filter(
                    archive=OuterRef("pk"), name=step_name, status=Status.COMPLETED
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
