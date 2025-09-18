from django.db.models import Exists, OuterRef, Q

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


def count_excluded_archives(categories):
    """
    Returns the count of Archives that do not belong to any of the predefined categories.

    :param categories: A dictionary defining the included and excluded steps for each category.
    """
    query = Q()

    for steps in categories.values():
        included_steps = steps.get("included", [])
        excluded_steps = steps.get("excluded", [])

        current_category = Q()
        for step_name in included_steps:
            current_category &= Q(
                Exists(
                    Step.objects.filter(
                        archive=OuterRef("pk"),
                        name=step_name,
                        status=Status.COMPLETED,
                    )
                )
            )

        for step_name in excluded_steps:
            current_category &= Q(
                ~Exists(
                    Step.objects.filter(
                        archive=OuterRef("pk"),
                        name=step_name,
                        status=Status.COMPLETED,
                    )
                )
            )

        query |= current_category

    return Archive.objects.exclude(query).count()
