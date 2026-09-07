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


class ScheduledHarvestStatisticsTests(APITestCase):
    def setUp(self):
        self.url = reverse("scheduled_harvest_statistics")

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

    def create_aip_archive(self, source, recid):
        """Creates an Archive whose state resolves to AIP, by completing
        one HARVEST step (has_sip) and one ARCHIVE step (has_aip)."""
        archive = Archive.objects.create(source=source, recid=recid)
        Step.objects.create(
            step_name=StepName.HARVEST,
            status=Status.COMPLETED,
            archive=archive,
        )
        Step.objects.create(
            step_name=StepName.ARCHIVE,
            status=Status.COMPLETED,
            archive=archive,
        )
        archive.refresh_from_db()
        return archive

    def get_row(self, data, name):
        return next(row for row in data if row["name"] == name)

    def test_no_harvest_run_yet(self):
        source = self.create_source()
        self.create_scheduled_harvest(source)

        response = self.client.get(self.url, format="json")
        row = self.get_row(response.data, source.longname)
        self.assertIsNone(row["last_harvest_time"])
        self.assertIsNone(row["grace_period_days"])
        self.assertEqual(row["preserved_unique_archives"], 0)

    def test_uses_latest_harvest_run(self):
        source = self.create_source()
        sh = self.create_scheduled_harvest(source)
        self.create_harvest_run(sh, grace_period_days=10)
        latest_run = self.create_harvest_run(sh, grace_period_days=30)

        response = self.client.get(self.url, format="json")
        row = self.get_row(response.data, source.longname)
        self.assertEqual(row["grace_period_days"], 30)

    def test_scope_full_when_no_extra_query(self):
        source = self.create_source()
        self.create_scheduled_harvest(source, extra_query=None)

        response = self.client.get(self.url, format="json")
        row = self.get_row(response.data, source.longname)
        self.assertEqual(row["scope"], "Full")

    def test_scope_partial_when_extra_query_set(self):
        source = self.create_source()
        self.create_scheduled_harvest(source, extra_query='q=title:"Digital Memory"')

        response = self.client.get(self.url, format="json")
        row = self.get_row(response.data, source.longname)
        self.assertEqual(row["scope"], "Partial")

    def test_counts_only_aip_archives_linked_to_this_scheduled_harvest(self):
        source = self.create_source()
        sh = self.create_scheduled_harvest(source)
        run = self.create_harvest_run(sh)
        batch = self.create_harvest_batch(run)

        linked_archive = self.create_aip_archive(source.name, "REC-1")
        batch.add_archive(linked_archive)

        self.create_aip_archive(source.name, "REC-2")

        response = self.client.get(self.url, format="json")
        row = self.get_row(response.data, source.longname)
        self.assertEqual(row["preserved_unique_archives"], 1)

    def test_excludes_archives_not_in_aip_state(self):
        source = self.create_source()
        sh = self.create_scheduled_harvest(source)
        run = self.create_harvest_run(sh)
        batch = self.create_harvest_batch(run)

        archive = Archive.objects.create(source=source.name, recid="REC-1")
        Step.objects.create(
            step_name=StepName.HARVEST, status=Status.COMPLETED, archive=archive
        )
        archive.refresh_from_db()
        batch.add_archive(archive)

        response = self.client.get(self.url, format="json")
        row = self.get_row(response.data, source.longname)
        self.assertEqual(row["preserved_unique_archives"], 0)

    def test_duplicate_recid_counted_once(self):
        source = self.create_source()
        sh = self.create_scheduled_harvest(source)
        run = self.create_harvest_run(sh)
        batch = self.create_harvest_batch(run)

        archive_v1 = self.create_aip_archive(source.name, "REC-1")
        archive_v2 = self.create_aip_archive(source.name, "REC-1")
        batch.add_archive(archive_v1)
        batch.add_archive(archive_v2)

        response = self.client.get(self.url, format="json")
        row = self.get_row(response.data, source.longname)
        self.assertEqual(row["preserved_unique_archives"], 1)

    def test_full_takes_priority_over_partial_same_source(self):
        source = self.create_source()
        partial_sh = self.create_scheduled_harvest(
            source, extra_query='q=title:"Digital Memory"'
        )
        self.create_harvest_run(partial_sh)
        full_sh = self.create_scheduled_harvest(source, extra_query=None)
        self.create_harvest_run(full_sh, grace_period_days=99)

        response = self.client.get(self.url, format="json")
        matching_rows = [row for row in response.data if row["name"] == source.longname]
        self.assertEqual(len(matching_rows), 1)
        self.assertEqual(matching_rows[0]["scope"], "Full")
        self.assertEqual(matching_rows[0]["grace_period_days"], 99)

    def test_most_recent_full_is_used_when_multiple_exist(self):
        source = self.create_source()
        old_full = self.create_scheduled_harvest(source, extra_query=None)
        self.create_harvest_run(old_full, grace_period_days=10)
        new_full = self.create_scheduled_harvest(source, extra_query=None)
        new_full.name = "Newer harvest"
        new_full.save()
        self.create_harvest_run(new_full, grace_period_days=50)

        response = self.client.get(self.url, format="json")
        matching_rows = [row for row in response.data if row["name"] == source.longname]
        self.assertEqual(len(matching_rows), 1)
        self.assertEqual(matching_rows[0]["grace_period_days"], 50)
