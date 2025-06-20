from datetime import timedelta
from unittest.mock import MagicMock

from django.apps import apps
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks import push_to_cta
from oais_platform.settings import (
    FTS_BACKOFF_LIMIT_IN_WEEKS,
    FTS_CONCURRENCY_LIMIT,
    FTS_WAIT_IN_HOURS,
)


class PushToCTATests(APITestCase):
    def setUp(self):
        self.app_config = apps.get_app_config("oais")
        self.fts = MagicMock()
        self.app_config.fts = self.fts

        self.archive = Archive.objects.create(path_to_aip="test/path")
        self.step = Step.objects.create(
            archive=self.archive, name=Steps.PUSH_TO_CTA, start_date=timezone.now()
        )

        self.backoff_archive = Archive.objects.create(path_to_aip="test/path")
        self.backoff_step = Step.objects.create(
            archive=self.backoff_archive,
            name=Steps.PUSH_TO_CTA,
            start_date=timezone.now() - timedelta(weeks=FTS_BACKOFF_LIMIT_IN_WEEKS),
        )

        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=FTS_WAIT_IN_HOURS, period=IntervalSchedule.HOURS
        )
        self.periodic_task = PeriodicTask.objects.create(
            interval=schedule,
            name=f"Push to CTA: {self.step.id}",
            task="push_to_cta",
        )
        self.backoff_periodic_task = PeriodicTask.objects.create(
            interval=schedule,
            name=f"Push to CTA: {self.backoff_step.id}",
            task="push_to_cta",
        )

    def test_push_to_cta_success(self):
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

    def test_push_to_cta_exception(self):
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

    def test_push_to_cta_wait(self):
        self.fts.number_of_transfers.return_value = FTS_CONCURRENCY_LIMIT
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.assertEqual(self.fts.number_of_transfers.call_count, 1)
        self.assertEqual(self.fts.push_to_cta.call_count, 0)
        self.assertTrue(
            PeriodicTask.objects.filter(name=f"Push to CTA: {self.step.id}")
        )

    def test_push_to_cta_retry_after_wait(self):
        self.fts.number_of_transfers.return_value = 0
        self.fts.push_to_cta.return_value = "test_job_id"
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

    def test_push_to_cta_backoff(self):
        push_to_cta.apply(
            args=[self.backoff_archive.id, self.backoff_step.id],
        )
        self.backoff_step.refresh_from_db()
        self.assertEqual(self.backoff_step.status, Status.FAILED)
        self.assertEqual(self.fts.push_to_cta.call_count, 0)
        self.assertFalse(
            PeriodicTask.objects.filter(
                name=f"Push to CTA: {self.backoff_step.id}"
            ).exists()
        )
