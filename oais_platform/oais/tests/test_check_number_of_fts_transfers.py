from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.apps import apps
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks import check_number_of_transfers
from oais_platform.settings import FTS_BACKOFF_IN_WEEKS, FTS_MAX_TRANSFERS


class CheckNumberOfTransfersTests(APITestCase):
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
            name=f"Check number of transfers: {self.step.id}",
            task="check_number_of_transfers",
        )

    @patch("oais_platform.oais.tasks.push_to_cta.delay")
    def test_check_number_of_transfers_retry(self, push_to_cta):
        self.fts.number_of_transfers.return_value = 0
        check_number_of_transfers.apply(
            args=[self.archive.id, self.step.id, timezone.now().isoformat()]
        )
        push_to_cta.assert_called_once()
        self.assertFalse(
            PeriodicTask.objects.filter(name=self.periodic_task.name).exists()
        )

    @patch("oais_platform.oais.tasks.push_to_cta.delay")
    def test_check_number_of_transfers_retry_wait(self, push_to_cta):
        self.fts.number_of_transfers.return_value = FTS_MAX_TRANSFERS
        check_number_of_transfers.apply(
            args=[self.archive.id, self.step.id, timezone.now().isoformat()]
        )
        push_to_cta.assert_not_called()
        self.assertTrue(
            PeriodicTask.objects.filter(name=self.periodic_task.name).exists()
        )

    @patch("oais_platform.oais.tasks.push_to_cta.delay")
    def test_check_number_of_transfers_retry_backoff(self, push_to_cta):
        self.fts.number_of_transfers.return_value = FTS_MAX_TRANSFERS
        start_time = timezone.now() - timedelta(weeks=FTS_BACKOFF_IN_WEEKS)
        check_number_of_transfers.apply(
            args=[self.archive.id, self.step.id, start_time.isoformat()]
        )
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        push_to_cta.assert_not_called()
        self.assertFalse(
            PeriodicTask.objects.filter(name=self.periodic_task.name).exists()
        )
