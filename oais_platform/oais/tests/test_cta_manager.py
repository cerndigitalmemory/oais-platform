from datetime import timedelta
from unittest.mock import MagicMock, patch

from django.apps import apps
from django.utils import timezone
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.cta import cta_manager
from oais_platform.settings import FTS_MAX_RETRY_COUNT, FTS_WAIT_LIMIT_IN_WEEKS


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
    def test_cta_manager_triggers_waiting_tasks(self, mock_push_to_cta):
        cta_manager.apply()
        mock_push_to_cta.assert_called_once_with(self.archive.id, self.step.id)

    def test_cta_manager_handles_finished_jobs(self):
        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": "job_id"})
        self.step.save()

        self.fts.job_statuses.return_value = [
            {"job_id": "job_id", "job_state": "FINISHED"}
        ]

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.COMPLETED)

    @patch("oais_platform.oais.tasks.cta.create_retry_step.apply_async")
    def test_cta_manager_handles_failed_jobs(self, mock_retry):
        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": "job_id"})
        self.step.save()

        self.fts.job_statuses.return_value = [
            {"job_id": "job_id", "job_state": "FAILED", "artifact": "test-artifact"}
        ]

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        mock_retry.assert_called_once()

    @patch("oais_platform.oais.tasks.cta.push_to_cta.delay")
    def test_cta_manager_concurrency_limit(self, mock_push_to_cta):
        for i in range(3):
            archive = Archive.objects.create(
                path_to_aip=f"basepath/aips/test/path/file_{i}.zip"
            )
            step = Step.objects.create(
                archive=archive,
                step_name=StepName.PUSH_TO_CTA,
                status=Status.WAITING,
                start_date=timezone.now(),
            )
            archive.last_step_id = step.id
            archive.save()

        cta_manager.apply()
        self.assertEqual(mock_push_to_cta.call_count, 2)

    def test_cta_manager_wait_limit(self):
        self.step.start_date = timezone.now() - timedelta(
            weeks=FTS_WAIT_LIMIT_IN_WEEKS + 1
        )
        self.step.save()

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertIn(
            "Wait limit reached", self.step.get_output_data().get("errormsg", "")
        )

    @patch("oais_platform.oais.tasks.cta.create_retry_step.apply_async")
    def test_cta_manager_retry(self, mock_retry_task):
        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": "job_id"})
        self.step.save()

        self.fts.job_statuses.return_value = [
            {
                "job_id": "job_id",
                "job_state": "FAILED",
            }
        ]

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        mock_retry_task.assert_called_once()
        self.assertTrue(self.step.get_output_data()["retrying"])

    @patch("oais_platform.oais.tasks.cta.create_retry_step.apply_async")
    def test_cta_manager_max_retry_count(self, mock_retry_task):
        input_step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
        )
        self.step.input_step = input_step
        self.step.set_input_data({"retry_count": FTS_MAX_RETRY_COUNT - 1})
        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": "job_id"})
        self.step.save()

        self.fts.job_statuses.return_value = [
            {"job_id": "job_id", "job_state": "FAILED"}
        ]

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertFalse(self.step.get_output_data()["retrying"])
        mock_retry_task.assert_not_called()

    @patch("oais_platform.oais.tasks.cta.push_to_cta.delay")
    def test_cta_manager_other_step_as_last(self, mock_push_to_cta):
        other_step = Step.objects.create(
            archive=self.archive, step_name=StepName.NOTIFY_SOURCE
        )
        self.archive.last_step_id = other_step.id
        self.archive.save()

        cta_manager.apply()

        mock_push_to_cta.assert_not_called()
