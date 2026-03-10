from unittest.mock import MagicMock, patch

from django.apps import apps
from django.utils import timezone
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.cta import cta_manager


class CTAManagerTests(APITestCase):
    def setUp(self):
        self.app_config = apps.get_app_config("oais")
        self.fts = MagicMock()
        self.app_config.fts = self.fts

        self.archive = Archive.objects.create(
            path_to_aip="basepath/aips/test/path/filename.zip"
        )
        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            status=Status.WAITING,
            start_date=timezone.now(),
        )
        self.step.set_input_data({"test": "test"})
        self.archive.set_last_step(self.step)
        self.step.step_type.concurrency_limit = 2
        self.step.step_type.save()

    @patch("oais_platform.oais.tasks.cta.push_to_cta.delay")
    def test_manager_triggers_waiting_tasks(self, mock_push_to_cta):
        cta_manager.apply()
        mock_push_to_cta.assert_called_once_with(self.archive.id, self.step.id)

    def test_manager_updates_finished_jobs(self):
        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": "job_123"})
        self.step.save()

        self.fts.job_statuses.return_value = [
            {"job_id": "job_123", "job_state": "FINISHED"}
        ]

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.COMPLETED)

    @patch("oais_platform.oais.tasks.cta.create_retry_step.apply_async")
    def test_manager_handles_failed_jobs(self, mock_retry):
        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": "job_456"})
        self.step.save()

        self.fts.job_statuses.return_value = [
            {"job_id": "job_456", "job_state": "FAILED", "artifact": "test-artifact"}
        ]

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        mock_retry.assert_called_once()
