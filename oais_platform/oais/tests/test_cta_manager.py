from unittest.mock import MagicMock, patch

from django.apps import apps
from django.utils import timezone
from rest_framework.test import APITestCase

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.cta import cta_manager
from oais_platform.settings import FTS_MAX_RETRY_COUNT


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
        self.assertTrue(self.step.output_data_json["retrying"])

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
        self.assertFalse(self.step.output_data_json["retrying"])
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

    def test_cta_manager_handles_missing_job_id(self):
        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": None})
        self.step.save()

        self.fts.job_statuses.return_value = []

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(self.step.failure_type, StepFailureType.MISSING_OUTPUT_DATA)

    def _setup_waiting_archive(self):
        self.step.step_type.concurrency_limit = 1
        self.step.step_type.save()

        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": "job_id"})
        self.step.save()

        waiting_archive = Archive.objects.create(
            path_to_aip="basepath/aips/test/path/waiting.zip"
        )
        waiting_step = Step.objects.create(
            archive=waiting_archive,
            step_name=StepName.PUSH_TO_CTA,
            status=Status.WAITING,
            start_date=timezone.now(),
        )
        waiting_archive.set_last_step(waiting_step)
        return waiting_archive, waiting_step

    @patch("oais_platform.oais.tasks.cta.push_to_cta.delay")
    def test_cta_manager_triggers_transfer_after_finished_job(self, mock_push_to_cta):
        waiting_archive, waiting_step = self._setup_waiting_archive()

        self.fts.job_statuses.return_value = [
            {"job_id": "job_id", "job_state": "FINISHED"}
        ]

        cta_manager.apply()
        mock_push_to_cta.assert_called_once_with(waiting_archive.id, waiting_step.id)

    @patch("oais_platform.oais.tasks.cta.push_to_cta.delay")
    def test_cta_manager_step_type_disabled(self, mock_push_to_cta):
        self.step.step_type.enabled = False
        self.step.step_type.save()
        self._setup_waiting_archive()

        self.fts.job_statuses.return_value = [
            {"job_id": "job_id", "job_state": "FINISHED"}
        ]

        cta_manager.apply()
        mock_push_to_cta.assert_not_called()

    @patch("oais_platform.oais.tasks.cta.create_retry_step.apply_async")
    @patch("oais_platform.oais.tasks.cta.push_to_cta.delay")
    def test_cta_manager_triggers_transfer_after_failed_job(
        self, mock_push_to_cta, mock_retry
    ):
        waiting_archive, waiting_step = self._setup_waiting_archive()

        self.fts.job_statuses.return_value = [
            {"job_id": "job_id", "job_state": "FAILED"}
        ]

        cta_manager.apply()
        mock_push_to_cta.assert_called_once_with(waiting_archive.id, waiting_step.id)

    @patch("oais_platform.oais.tasks.cta.create_retry_step.apply_async")
    def test_cta_manager_handles_not_found_jobs(self, mock_retry):
        self.step.status = Status.IN_PROGRESS
        self.step.set_output_data({"fts_job_id": "job_id"})
        self.step.save()

        self.fts.job_statuses.side_effect = Exception(
            'Client error: No job with the id "job_id" has been found'
        )

        cta_manager.apply()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertIn(
            "was not found", self.step.output_data_json["FTS status"]["errormsg"]
        )
        mock_retry.assert_called_once()
        self.assertTrue(self.step.output_data_json["retrying"])
