from django.db.models import (
    Avg,
    Count,
    DurationField,
    Exists,
    ExpressionWrapper,
    F,
    Max,
    Min,
    OuterRef,
)
from django.db.models.functions import TruncDate

from oais_platform.oais.enums import COMPLETED_STATUSES
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
                    step_name=step_name,
                    status=Status.COMPLETED,
                )
            )
        )

    for step_name in exclude_steps:
        archives = archives.filter(
            ~Exists(
                Step.objects.filter(
                    archive=OuterRef("pk"),
                    step_name=step_name,
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


def avg_duration_per_day(
    collection_id=None, step_name=None, statuses=COMPLETED_STATUSES
):
    """
    Calculate the average duration of completed steps per day for a specific collection.
    """
    steps = Step.objects.filter(
        step_name=step_name,
        status__in=statuses,
    )

    if collection_id:
        steps = steps.filter(archive__archive_collections__id=collection_id)
    return (
        steps.exclude(start_date__isnull=True, finish_date__isnull=True)
        .annotate(
            day=TruncDate("start_date"),
            duration=ExpressionWrapper(
                F("finish_date") - F("start_date"), output_field=DurationField()
            ),
        )
        .values("day")
        .annotate(
            avg_duration=Avg("duration"),
            min_duration=Min("duration"),
            max_duration=Max("duration"),
            count=Count("id"),
            avg_size=Avg("archive__sip_size"),
        )
        .order_by("day")
    )
