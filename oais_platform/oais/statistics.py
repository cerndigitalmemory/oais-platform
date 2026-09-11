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
    Q,
    Subquery,
    Value,
)
from django.db.models.functions import Coalesce, TruncDate
from django.utils import timezone

from oais_platform.oais.enums import (
    COMPLETED_STATUSES,
    ArchiveState,
    Status,
    StepFailureType,
    StepName,
)
from oais_platform.oais.models import (
    Archive,
    HarvestRun,
    ScheduledHarvest,
    Source,
    Status,
    Step,
)


def _completed_step_exists(step_name):
    return Exists(
        Step.objects.filter(
            archive=OuterRef("pk"),
            step_name=step_name,
            status=Status.COMPLETED,
        )
    )


def step_statistics_counts():
    """
    Returns the count of Archives in each pipeline-step category.
    """
    not_pushed = Q(has_cta=False, has_invenio=False)

    counts = Archive.objects.annotate(
        has_cta=_completed_step_exists(StepName.PUSH_TO_CTA),
        has_invenio=_completed_step_exists(StepName.INVENIO_RDM_PUSH),
    ).aggregate(
        total=Count("pk"),
        staged_count=Count(
            "pk",
            filter=Q(staged=True),
        ),
        harvested_count=Count(
            "pk",
            filter=Q(staged=False, state=ArchiveState.SIP) & not_pushed,
        ),
        harvested_preserved_count=Count(
            "pk",
            filter=Q(staged=False, state=ArchiveState.AIP) & not_pushed,
        ),
        harvested_preserved_tape_count=Count(
            "pk",
            filter=Q(
                staged=False,
                state=ArchiveState.AIP,
                has_cta=True,
                has_invenio=False,
            ),
        ),
        harvested_preserved_registry_count=Count(
            "pk",
            filter=Q(
                staged=False,
                state=ArchiveState.AIP,
                has_cta=False,
                has_invenio=True,
            ),
        ),
        harvested_preserved_tape_registry_count=Count(
            "pk",
            filter=Q(
                staged=False,
                state=ArchiveState.AIP,
                has_cta=True,
                has_invenio=True,
            ),
        ),
    )

    total = counts.pop("total")
    counts["others_count"] = total - sum(counts.values())
    return counts


def latest_steps(steps=None):
    """
    Returns the most recent Step per archive and step_type.
    Pass a pre-filtered queryset to scope the result.
    """
    if steps is None:
        steps = Step.objects.all()
    latest = (
        Step.objects.filter(
            archive=OuterRef("archive"),
            step_type=OuterRef("step_type"),
        )
        .order_by("-start_date", "-create_date")
        .values("id")[:1]
    )
    return steps.filter(step_type__isnull=False, id=Subquery(latest))


def count_steps_by_status():
    """
    Returns the count of current Steps grouped by step name and status.
    """
    rows = (
        latest_steps().values("step_type__name", "status").annotate(count=Count("id"))
    )
    counts = {(row["step_type__name"], row["status"]): row["count"] for row in rows}
    return [
        {
            "step": step,
            "status": label,
            "count": counts.get((step, status_value), 0),
        }
        for step in StepName.values
        for status_value, label in Status.choices
    ]


def failures_by_type(steps=None):
    """
    Returns the latest failed Steps grouped by step name and failure type, with counts.
    Pass a pre-filtered queryset to scope the result.
    """
    return (
        latest_steps(steps)
        .filter(status=Status.FAILED)
        .annotate(
            grouped_failure_type=Coalesce("failure_type", Value(StepFailureType.OTHER))
        )
        .values("step_type__name", "grouped_failure_type")
        .annotate(count=Count("id"))
    )


def count_failures_by_type():
    """
    Returns the count of current failed Steps grouped by step name and failure type.
    """
    return [
        {
            "step": row["step_type__name"],
            "failure_type": row["grouped_failure_type"],
            "count": row["count"],
        }
        for row in failures_by_type()
    ]


def _current_duration():
    """
    Returns an expression for annotating the duration of a Step.
    """
    return ExpressionWrapper(
        Coalesce(
            F("finish_date") - F("start_date"),
            timezone.now() - Coalesce(F("start_date"), F("create_date")),
        ),
        output_field=DurationField(),
    )


def avg_in_progress_duration_by_step():
    """
    Returns the average duration (seconds) of current in-progress Steps per step name.
    """
    durations = {
        row["step_type__name"]: row["avg_duration"]
        for row in (
            latest_steps()
            .filter(status__in=[Status.IN_PROGRESS, Status.SUBMITTED])
            .annotate(duration=_current_duration())
            .values("step_type__name")
            .annotate(avg_duration=Avg("duration"))
        )
    }
    return [
        {
            "step": step,
            "avg_duration": (
                float(f"{durations[step].total_seconds():.2f}")
                if durations.get(step)
                else None
            ),
        }
        for step in StepName.values
    ]


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
    return list(
        steps.exclude(start_date__isnull=True, finish_date__isnull=True)
        .annotate(
            day=TruncDate("finish_date"),
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
            min_size=Min("archive__sip_size"),
            max_size=Max("archive__sip_size"),
        )
        .order_by("-day")
    )


def _scheduled_harvest_to_dict(scheduled_harvest):
    last_run = (
        HarvestRun.objects.filter(scheduled_harvest=scheduled_harvest)
        .order_by("-created_at")
        .first()
    )

    archive_count = (
        Archive.objects.filter(
            harvest_batches__harvest_run__scheduled_harvest=scheduled_harvest,
            state__in=[ArchiveState.SIP, ArchiveState.AIP],
        )
        .distinct()
        .count()
    )

    return {
        "name": scheduled_harvest.name,
        "grace_period_days": last_run.grace_period_days if last_run else None,
        "last_run_date": last_run.created_at if last_run else None,
        "scope": "Partial" if scheduled_harvest.extra_query else "Full",
        "archive_count": archive_count,
    }


def harvested_sources_overview():
    """
    Returns, for each Source with at least one harvested Archive, the
    total number harvested, the latest harvest time, and the list of
    its ScheduledHarvest (empty if none exist).
    """

    source_stats = (
        Archive.objects.filter(
            state__in=[ArchiveState.SIP, ArchiveState.AIP],
        )
        .values("source")
        .annotate(
            total_harvested=Count("recid", distinct=True),
            latest_harvested=Max("steps__finish_date"),
        )
    )

    result = []

    for row in source_stats:
        source_name = row["source"]

        try:
            source_longname = Source.objects.get(name=source_name).longname
        except Source.DoesNotExist:
            # In case if Source has been deleted/renamed
            source_longname = source_name

        scheduled_harvests = ScheduledHarvest.objects.filter(source__name=source_name)

        result.append(
            {
                "source": source_longname,
                "total_harvested": row["total_harvested"],
                "latest_harvested": row["latest_harvested"],
                "scheduled_harvests": [
                    _scheduled_harvest_to_dict(scheduled_harvest)
                    for scheduled_harvest in scheduled_harvests
                ],
            }
        )

    return result
