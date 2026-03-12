"""
Execute in: python3 manage.py shell
This script calculates the average duration of completed archive steps per day for a specific collection.
"""

from django.db.models import Avg, DurationField, ExpressionWrapper, F
from django.db.models.functions import TruncDate

from oais_platform.oais.models import Collection, Status, Step, StepName

collection = Collection.objects.get(id=89)

daily_avg = (
    Step.objects.filter(
        step_name=StepName.ARCHIVE,
        status__in=[Status.COMPLETED, Status.COMPLETED_WITH_WARNINGS],
        archive__in=collection.archives.all(),
    )
    .exclude(start_date__isnull=True, finish_date__isnull=True)
    .annotate(
        day=TruncDate("start_date"),
        duration=ExpressionWrapper(
            F("finish_date") - F("start_date"), output_field=DurationField()
        ),
    )
    .values("day")
    .annotate(avg_duration=Avg("duration"))
    .order_by("day")
)

for entry in daily_avg:
    avg_str = str(entry["avg_duration"]).split(".")[0]
    print(f"{entry['day']}: {avg_str}")
