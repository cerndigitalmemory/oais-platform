from unittest.mock import MagicMock

from django.apps import apps
from django_celery_beat.models import PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks import push_to_cta
from oais_platform.settings import FTS_CONCURRENCY_LIMIT


class PushToCTATests(APITestCase):
    def setUp(self):
        self.app_config = apps.get_app_config("oais")
        self.fts = MagicMock()
        self.app_config.fts = self.fts

        self.archive = Archive.objects.create(path_to_aip="test/path")
        self.step = Step.objects.create(archive=self.archive, name=Steps.PUSH_TO_CTA)

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
            PeriodicTask.objects.filter(
                name=f"Check number of transfers: {self.step.id}"
            )
        )

    def test_push_to_cta_skip_check(self):
        self.fts.number_of_transfers.return_value = FTS_CONCURRENCY_LIMIT
        self.fts.push_to_cta.return_value = "test_job_id"
        push_to_cta.apply(
            args=[self.archive.id, self.step.id],
            kwargs={"skip_concurrency_check": True},
        )
        self.assertEqual(self.fts.number_of_transfers.call_count, 0)
        self.assertEqual(self.fts.push_to_cta.call_count, 1)
        self.assertTrue(
            PeriodicTask.objects.filter(name=f"FTS job status for step: {self.step.id}")
        )
