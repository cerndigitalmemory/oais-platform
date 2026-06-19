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
)
from django.db.models.functions import TruncDate

from oais_platform.oais.enums import COMPLETED_STATUSES, ArchiveState, StepName
from oais_platform.oais.models import Archive, Status, Step


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
        .values("step_type__name", "failure_type")
        .annotate(count=Count("id"))
    )


def count_failures_by_type():
    """
    Returns the count of current failed Steps grouped by step name and failure type.
    """
    return [
        {
            "step": row["step_type__name"],
            "failure_type": row["failure_type"],
            "count": row["count"],
        }
        for row in failures_by_type()
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
