from datetime import timedelta
from unittest.mock import MagicMock, Mock, patch

from django.apps import apps
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.cta import push_to_cta
from oais_platform.settings import FTS_WAIT_IN_HOURS, FTS_WAIT_LIMIT_IN_WEEKS


class MockedGError(Exception):
    def __init__(self, message, code):
        super().__init__(message)
        self.message = message
        self.code = code


@patch("oais_platform.oais.tasks.cta.gfal2")
class PushToCTATests(APITestCase):
    MockedGError = MockedGError

    def setUp(self):
        self.app_config = apps.get_app_config("oais")
        self.fts = MagicMock()
        self.app_config.fts = self.fts

        self.archive = Archive.objects.create(path_to_aip="test/path")
        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            start_date=timezone.now(),
        )
        self.step.step_type.concurrency_limit = 5
        self.step.step_type.save()

        self.wait_limit_archive = Archive.objects.create(path_to_aip="test/path")
        self.wait_limit_step = Step.objects.create(
            archive=self.wait_limit_archive,
            step_name=StepName.PUSH_TO_CTA,
            start_date=timezone.now() - timedelta(weeks=FTS_WAIT_LIMIT_IN_WEEKS),
        )

        self.schedule, _ = IntervalSchedule.objects.get_or_create(
            every=FTS_WAIT_IN_HOURS, period=IntervalSchedule.HOURS
        )

    def _setup_gfal2_mocks(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.stat.side_effect = self.MockedGError("404 File not found", 404)
        mock_gfal2.GError = self.MockedGError
        mock_gfal2.creat_context.return_value = mock_ctx

    def test_push_to_cta_success(self, mock_gfal2):
        self._setup_gfal2_mocks(mock_gfal2)
        self.fts.number_of_transfers.return_value = 0
        self.fts.push_to_cta.return_value = "test_job_id"
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.step.refresh_from_db()
        self.assertEqual(self.fts.push_to_cta.call_count, 1)
        self.assertEqual(self.step.status, Status.IN_PROGRESS)
        self.assertTrue(
            PeriodicTask.objects.filter(
                name=f"FTS job status for step: {self.step.id}"
            ).exists()
        )

    def test_push_to_cta_exception(self, mock_gfal2):
        self._setup_gfal2_mocks(mock_gfal2)
        self.fts.number_of_transfers.return_value = 0
        self.fts.push_to_cta.side_effect = Exception()
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.step.refresh_from_db()
        self.assertEqual(self.fts.push_to_cta.call_count, 2)
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertFalse(
            PeriodicTask.objects.filter(
                name=f"FTS job status for step: {self.step.id}"
            ).exists()
        )

    def test_push_to_cta_wait(self, mock_gfal2):
        self._setup_gfal2_mocks(mock_gfal2)
        self.fts.number_of_transfers.return_value = (
            self.step.step_type.concurrency_limit + 1
        )
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.assertEqual(self.fts.number_of_transfers.call_count, 1)
        self.assertEqual(self.fts.push_to_cta.call_count, 0)
        self.assertTrue(
            PeriodicTask.objects.filter(name=f"Push to CTA: {self.step.id}")
        )

    def test_push_to_cta_retry_after_wait(self, mock_gfal2):
        self._setup_gfal2_mocks(mock_gfal2)
        self.fts.number_of_transfers.return_value = 0
        self.fts.push_to_cta.return_value = "test_job_id"
        PeriodicTask.objects.create(
            interval=self.schedule,
            name=f"Push to CTA: {self.step.id}",
            task="push_to_cta",
        )
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.step.refresh_from_db()
        self.assertEqual(self.fts.push_to_cta.call_count, 1)
        self.assertEqual(self.step.status, Status.IN_PROGRESS)
        self.assertFalse(
            PeriodicTask.objects.filter(name=f"Push to CTA: {self.step.id}").exists()
        )
        self.assertTrue(
            PeriodicTask.objects.filter(
                name=f"FTS job status for step: {self.step.id}"
            ).exists()
        )

    def test_push_to_cta_wait_limit(self, mock_gfal2):
        self._setup_gfal2_mocks(mock_gfal2)
        PeriodicTask.objects.create(
            interval=self.schedule,
            name=f"Push to CTA: {self.step.id}",
            task="push_to_cta",
        )
        push_to_cta.apply(
            args=[self.wait_limit_archive.id, self.wait_limit_step.id],
        )
        self.wait_limit_step.refresh_from_db()
        self.assertEqual(self.wait_limit_step.status, Status.FAILED)
        self.assertEqual(self.fts.push_to_cta.call_count, 0)
        self.assertFalse(
            PeriodicTask.objects.filter(
                name=f"Push to CTA: {self.wait_limit_step.id}"
            ).exists()
        )
