from datetime import timedelta
from unittest.mock import MagicMock

from django.apps import apps
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks.cta import push_to_cta
from oais_platform.settings import (
    FTS_CONCURRENCY_LIMIT,
    FTS_WAIT_IN_HOURS,
    FTS_WAIT_LIMIT_IN_WEEKS,
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

        self.wait_limit_archive = Archive.objects.create(path_to_aip="test/path")
        self.wait_limit_step = Step.objects.create(
            archive=self.wait_limit_archive,
            name=Steps.PUSH_TO_CTA,
            start_date=timezone.now() - timedelta(weeks=FTS_WAIT_LIMIT_IN_WEEKS),
        )

        self.schedule, _ = IntervalSchedule.objects.get_or_create(
            every=FTS_WAIT_IN_HOURS, period=IntervalSchedule.HOURS
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

    def test_push_to_cta_wait_limit(self):
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
