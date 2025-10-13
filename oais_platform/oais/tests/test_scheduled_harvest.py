from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    BatchStatus,
    Collection,
    HarvestBatch,
    HarvestRun,
    Profile,
    ScheduledHarvest,
    Source,
    Status,
    Step,
    StepName,
    StepType,
)
from oais_platform.oais.tasks.scheduled_harvest import batch_harvest, scheduled_harvest
from oais_platform.oais.tests.utils import TestSource


class ScheduledHarvestTests(APITestCase):
    def setUp(self):
        self.system_user = Profile.objects.get(system=True).user
        self.source = Source.objects.create(
            name="Test Source", enabled=True, classname="TestSource"
        )
        self.pipeline = [StepName.HARVEST]
        self.schedule = ScheduledHarvest.objects.create(
            name="Test Schedule",
            source=self.source,
            enabled=True,
            pipeline=self.pipeline,
        )

    def batch_setup(self):
        self.collection = Collection.objects.create(title="Test Collection")
        self.run = HarvestRun.objects.create(
            scheduled_harvest=self.schedule,
            source=self.source,
            pipeline=self.pipeline,
            query_start_time=None,
            query_end_time=datetime.now(timezone.utc),
            collection=self.collection,
        )
        self.batch = HarvestBatch.objects.create(
            harvest_run=self.run,
            status=BatchStatus.PENDING,
            records=[
                {
                    "source_url": "https://example.com/record/1",
                    "recid": "1",
                    "title": "test",
                    "authors": [],
                    "source": "test",
                }
            ],
            batch_number=1,
        )

    def test_scheduled_harvest_not_found(self):
        with self.assertLogs(level="ERROR") as log:
            result = scheduled_harvest.apply(args=[999])
            self.assertIn("ScheduledHarvest with id 999 does not exist.", log.output[0])
            self.assertIsNone(result.result)

    def test_scheduled_harvest_disabled(self):
        self.schedule.enabled = False
        self.schedule.save()
        with self.assertLogs(level="WARNING") as log:
            result = scheduled_harvest.apply(args=[self.schedule.id])
            self.assertIn(
                f"ScheduledHarvest with id {self.schedule.id} is disabled.",
                log.output[0],
            )
            self.assertIsNone(result.result)

    def test_scheduled_harvest_no_system_user(self):
        self.system_user.profile.delete()
        self.system_user.delete()
        with self.assertLogs(level="ERROR") as log:
            result = scheduled_harvest.apply(args=[self.schedule.id])
            self.assertIn(
                "System user does not exist - cannot execute scheduled harvest.",
                log.output[0],
            )
            self.assertIsNone(result.result)

    @patch("oais_platform.oais.tasks.scheduled_harvest.batch_harvest")
    def test_scheduled_harvest_first_run_no_records(self, mock_batch):
        with self.assertLogs(level="INFO") as log:
            with patch(
                "oais_platform.oais.tasks.scheduled_harvest.get_source"
            ) as mock_get_source:
                mock_instance = MagicMock()
                mock_instance.get_records_to_harvest.return_value = iter([])
                mock_get_source.return_value = mock_instance
                scheduled_harvest.apply(args=[self.schedule.id])
                self.assertIn(
                    "does not have API key set for the given source, only public records will be available.",
                    log.output[1],
                )
                self.assertIn(
                    f"First harvest for source {self.schedule.source.name}.",
                    log.output[2],
                )
                run_obj = HarvestRun.objects.filter(
                    scheduled_harvest=self.schedule
                ).first()
                self.assertIsNotNone(run_obj)
                self.assertEqual(run_obj.source.id, self.source.id)
                self.assertEqual(run_obj.pipeline, self.pipeline)
                self.assertIsNone(run_obj.query_start_time)
                self.assertIn(
                    "No records were harvested during this run.", log.output[3]
                )
                self.assertFalse(Collection.objects.exists())
                self.assertFalse(HarvestBatch.objects.exists())
                mock_batch.assert_not_called()

    @patch("oais_platform.oais.tasks.scheduled_harvest.batch_harvest.delay")
    def test_scheduled_harvest_success(self, mock_batch):
        ApiKey.objects.create(source=self.source, user=self.system_user, key="testkey")
        last_run = HarvestRun.objects.create(
            scheduled_harvest=self.schedule,
            source=self.source,
            pipeline=self.pipeline,
            query_start_time=datetime.now(timezone.utc).replace(microsecond=0),
        )
        with self.assertLogs(level="INFO") as log:
            with patch(
                "oais_platform.oais.tasks.scheduled_harvest.get_source"
            ) as mock_get_source:
                mock_get_source.return_value = TestSource()
                scheduled_harvest.apply(args=[self.schedule.id])
                self.assertIn(
                    f"Last harvest run for source {self.source.name} was until {last_run.query_end_time}.",
                    log.output[1],
                )
                run_obj = (
                    HarvestRun.objects.filter(scheduled_harvest=self.schedule)
                    .order_by("-created_at")
                    .first()
                )
                self.assertIsNotNone(run_obj)
                self.assertEqual(run_obj.source.id, self.source.id)
                self.assertEqual(run_obj.pipeline, self.pipeline)
                self.assertIn(
                    f"Number of IDs to harvest for source {self.source.name}: 1",
                    log.output[2],
                )
                self.assertTrue(Collection.objects.exists())
                self.assertTrue(HarvestBatch.objects.exists())
                mock_batch.assert_called_once_with(HarvestBatch.objects.last().id)

    def test_batch_harvest_not_existing(self):
        with self.assertLogs(level="ERROR") as log:
            result = batch_harvest.apply(args=[999])
            self.assertIn("HarvestBatch with id 999 does not exist.", log.output[0])
            self.assertIsNone(result.result)

    def test_batch_harvest_no_system_user(self):
        self.batch_setup()
        self.system_user.profile.delete()
        self.system_user.delete()
        with self.assertLogs(level="ERROR") as log:
            result = batch_harvest.apply(args=[self.batch.id])
            self.assertIn(
                "System user does not exist - cannot execute batch harvest.",
                log.output[0],
            )
            self.assertIsNone(result.result)

    def test_batch_harvest_blocked(self):
        self.batch_setup()
        self.batch.status = BatchStatus.BLOCKED
        self.batch.save()
        with self.assertLogs(level="ERROR") as log:
            result = batch_harvest.apply(args=[self.batch.id])
            self.assertIn(
                "has a blocked/failed batch, further batches will not be processed.",
                log.output[0],
            )
            self.assertIsNone(result.result)

    def test_batch_harvest_previous_failed(self):
        self.batch_setup()
        self.batch.status = BatchStatus.FAILED
        self.batch.save()
        new_batch = HarvestBatch.objects.create(
            harvest_run=self.run,
            status=BatchStatus.PENDING,
            records=[
                {
                    "source_url": "https://example.com/record/1",
                    "recid": "1",
                    "title": "test",
                    "authors": [],
                    "source": "test",
                }
            ],
            batch_number=2,
        )
        with self.assertLogs(level="ERROR") as log:
            result = batch_harvest.apply(args=[new_batch.id])
            self.assertIn(
                "has a blocked/failed batch, further batches will not be processed.",
                log.output[0],
            )
            self.assertIsNone(result.result)

    def test_batch_harvest_success(self):
        self.batch_setup()
        with patch(
            "oais_platform.oais.tasks.scheduled_harvest.chord"
        ) as mock_chord, patch(
            "oais_platform.oais.tasks.scheduled_harvest.execute_pipeline"
        ) as mock_execute_pipeline:
            with self.assertLogs(level="INFO") as log:
                mock_chord.return_value = MagicMock()
                fake_step = MagicMock(name="step")
                fake_sig = MagicMock(name="sig")
                mock_execute_pipeline.return_value = (fake_step, fake_sig)

                batch_harvest.apply(args=[self.batch.id])
                self.assertIn(
                    "does not have API key set for the given source",
                    log.output[0],
                )
                self.batch.refresh_from_db()
                self.assertEqual(self.batch.status, BatchStatus.IN_PROGRESS)
                archives = self.batch.archives
                self.assertEqual(archives.count(), 1)
                archive = archives.first()
                self.assertIn(
                    archive.id, self.collection.archives.values_list("id", flat=True)
                )
                self.assertEqual(archive.source, self.source.name)
                step_names = [
                    Step.objects.get(id=step_id).step_type.name
                    for step_id in archive.pipeline_steps
                ]
                self.assertEqual(step_names, self.pipeline)
                mock_execute_pipeline.assert_called_once_with(
                    archive.id, None, return_signature=True
                )
                mock_chord.assert_called_once_with([fake_sig])

    def test_batch_archive_counts(self):
        self.batch_setup()
        archive = self.create_batch_archive()
        self.create_batch_step(archive, StepName.HARVEST, Status.COMPLETED)
        self.create_batch_step(archive, StepName.VALIDATION, Status.COMPLETED)
        self.create_batch_step(archive, StepName.CHECKSUM, Status.COMPLETED)
        archive2 = self.create_batch_archive()
        self.create_batch_step(archive2, StepName.HARVEST, Status.COMPLETED)
        self.create_batch_step(archive2, StepName.VALIDATION, Status.FAILED)
        self.create_batch_step(archive2, StepName.CHECKSUM, Status.WAITING)
        archive3 = self.create_batch_archive()
        self.create_batch_step(archive3, StepName.HARVEST, Status.COMPLETED)
        self.create_batch_step(archive3, StepName.VALIDATION, Status.COMPLETED)
        self.create_batch_step(archive3, StepName.CHECKSUM, Status.IN_PROGRESS)
        archive4 = self.create_batch_archive()
        self.create_batch_step(archive4, StepName.HARVEST, Status.COMPLETED)
        self.create_batch_step(archive4, StepName.VALIDATION, Status.COMPLETED)
        self.create_batch_step(archive4, StepName.CHECKSUM, Status.COMPLETED)
        self.create_batch_step(archive4, StepName.ARCHIVE, Status.WAITING)

        self.assertEqual(self.batch.completed, 1)
        self.assertEqual(self.batch.failed, 1)
        self.assertEqual(self.batch.archives.count(), 4)

    def create_batch_archive(self):
        archive = Archive.objects.create(
            recid="1",
            title="test",
            source="test",
            source_url="https://example.com/record/1",
            requester=self.system_user,
            approver=self.system_user,
        )
        self.collection.add_archive(archive)
        return archive

    def create_batch_step(self, archive, name, status):
        Step.objects.create(
            archive=archive,
            step_name=name,
            initiated_by_harvest_batch=self.batch,
            status=status,
        )
