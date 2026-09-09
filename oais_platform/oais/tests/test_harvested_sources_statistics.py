from django.urls import reverse
from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    Archive,
    HarvestBatch,
    HarvestRun,
    ScheduledHarvest,
    Source,
    Status,
    Step,
    StepName,
)


class HarvestedSourcesStatisticsTest(APITestCase):
    def setUp(self):
        self.url = reverse("harvested_sources_statistics")

    def create_source(self, name="test-source"):
        return Source.objects.create(
            name=name,
            longname=f"{name} long name",
            api_url=f"https://{name}.example.com/api",
            classname="Invenio",
        )

    def create_scheduled_harvest(
        self, source, enabled=True, extra_query=None, name=None
    ):
        return ScheduledHarvest.objects.create(
            name=name
            or f"Harvest for {source.name} {ScheduledHarvest.objects.count()}",
            source=source,
            enabled=enabled,
            extra_query=extra_query,
        )

    def create_harvest_run(
        self, scheduled_harvest, grace_period_days=0, extra_query=None
    ):
        return HarvestRun.objects.create(
            source=scheduled_harvest.source,
            scheduled_harvest=scheduled_harvest,
            grace_period_days=grace_period_days,
            extra_query=extra_query,
        )

    def create_harvest_batch(self, harvest_run, batch_number=1):
        return HarvestBatch.objects.create(
            harvest_run=harvest_run,
            batch_number=batch_number,
        )

    def create_harvested_archive(self, source, recid):
        """Creates an Archive with a completed HARVEST step (has_sip)."""
        archive = Archive.objects.create(source=source, recid=recid)
        Step.objects.create(
            step_name=StepName.HARVEST,
            status=Status.COMPLETED,
            archive=archive,
        )
        archive.refresh_from_db()
        return archive

    def get_source_row(self, data, source_longname):
        return next(row for row in data if row["source"] == source_longname)

    def test_source_uses_longname_and_total_harvested_count(self):
        source = self.create_source()
        self.create_harvested_archive(source.name, "REC-1")
        self.create_harvested_archive(source.name, "REC-2")

        response = self.client.get(self.url, format="json")
        row = self.get_source_row(response.data, source.longname)
        self.assertEqual(row["total_harvested"], 2)

    def test_duplicate_recid_counted_once_in_total_harvested(self):
        source = self.create_source()
        self.create_harvested_archive(source.name, "REC-1")
        self.create_harvested_archive(source.name, "REC-1")

        response = self.client.get(self.url, format="json")
        row = self.get_source_row(response.data, source.longname)
        self.assertEqual(row["total_harvested"], 1)

    def test_source_with_no_scheduled_harvest_has_empty_list(self):
        source = self.create_source()
        self.create_harvested_archive(source.name, "REC-1")

        response = self.client.get(self.url, format="json")
        row = self.get_source_row(response.data, source.longname)
        self.assertEqual(row["scheduled_harvests"], [])

    def test_scheduled_harvest_with_no_run_yet(self):
        source = self.create_source()
        self.create_harvested_archive(source.name, "REC-1")
        self.create_scheduled_harvest(source)

        response = self.client.get(self.url, format="json")
        row = self.get_source_row(response.data, source.longname)
        sh_row = row["scheduled_harvests"][0]
        self.assertIsNone(sh_row["grace_period_days"])
        self.assertIsNone(sh_row["last_run_date"])
        self.assertEqual(sh_row["archive_count"], 0)

    def test_archive_count_only_counts_archives_linked_to_this_scheduled_harvest(self):
        source = self.create_source()
        sh = self.create_scheduled_harvest(source)
        run = self.create_harvest_run(sh)
        batch = self.create_harvest_batch(run)

        linked_archive = self.create_harvested_archive(source.name, "REC-1")
        batch.add_archive(linked_archive)

        # Not linked to any batch -> should not count for this ScheduledHarvest
        self.create_harvested_archive(source.name, "REC-2")

        response = self.client.get(self.url, format="json")
        row = self.get_source_row(response.data, source.longname)
        self.assertEqual(row["scheduled_harvests"][0]["archive_count"], 1)

    def test_uses_latest_harvest_run(self):
        source = self.create_source()
        self.create_harvested_archive(source.name, "REC-1")
        sh = self.create_scheduled_harvest(source)
        self.create_harvest_run(sh, grace_period_days=10)
        latest_run = self.create_harvest_run(sh, grace_period_days=30)

        response = self.client.get(self.url, format="json")
        row = self.get_source_row(response.data, source.longname)
        sh_row = row["scheduled_harvests"][0]
        self.assertEqual(sh_row["grace_period_days"], 30)
        self.assertEqual(sh_row["last_run_date"], latest_run.created_at)
