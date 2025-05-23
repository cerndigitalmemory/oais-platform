from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.apps import apps
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks import retry_push_to_cta
from oais_platform.settings import FTS_MAX_TRANSFERS


class RetryPushToCTATests(APITestCase):
    def setUp(self):
        self.app_config = apps.get_app_config("oais")
        self.fts = MagicMock()
        self.app_config.fts = self.fts

        self.archive = Archive.objects.create()
        self.step = Step.objects.create(archive=self.archive, name=Steps.PUSH_TO_CTA)
        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=1, period=IntervalSchedule.HOURS
        )
        self.periodic_task = PeriodicTask.objects.create(
            interval=schedule,
            name=f"Retry push to CTA: {self.step.id}",
            task="retry_push_to_cta",
        )

    @patch("oais_platform.oais.tasks.push_to_cta.delay")
    def test_retry_push_to_cta_retry(self, push_to_cta):
        self.fts.number_of_transfers.return_value = 0
        retry_push_to_cta.apply(args=[self.archive.id, self.step.id])
        push_to_cta.assert_called_once()
        self.assertFalse(
            PeriodicTask.objects.filter(name=self.periodic_task.name).exists()
        )

    @patch("oais_platform.oais.tasks.push_to_cta.delay")
    def test_retry_push_to_cta_retry_wait(self, push_to_cta):
        self.fts.number_of_transfers.return_value = FTS_MAX_TRANSFERS
        retry_push_to_cta.apply(args=[self.archive.id, self.step.id])
        push_to_cta.assert_not_called()
        self.assertTrue(
            PeriodicTask.objects.filter(name=self.periodic_task.name).exists()
        )

    @patch("oais_platform.oais.tasks.push_to_cta.delay")
    def test_retry_push_to_cta_retry_backoff(self, push_to_cta):
        self.fts.number_of_transfers.return_value = FTS_MAX_TRANSFERS
        week_ago = timezone.now() - timedelta(weeks=1)
        retry_push_to_cta.apply(
            args=[self.archive.id, self.step.id, None, week_ago.isoformat()]
        )
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        push_to_cta.assert_not_called()
        self.assertFalse(
            PeriodicTask.objects.filter(name=self.periodic_task.name).exists()
        )
