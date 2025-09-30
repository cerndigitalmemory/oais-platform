import json
from unittest.mock import MagicMock, patch

from django.apps import apps
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.cta import check_fts_job_status
from oais_platform.settings import FTS_MAX_RETRY_COUNT


class CheckFTSJobStatusTests(APITestCase):
    def setUp(self):
        self.app_config = apps.get_app_config("oais")
        self.fts = MagicMock()
        self.app_config.fts = self.fts

        self.archive = Archive.objects.create()
        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            input_data=json.dumps({"test": True}),
        )
        self.step.set_output_data({"artifact": {"artifact_name": "FTS Job"}})
        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=1, period=IntervalSchedule.HOURS
        )
        self.periodic_task = PeriodicTask.objects.create(
            interval=schedule,
            name=f"FTS job status for step: {self.step.id}",
            task="check_fts_job_status",
        )

    @patch("oais_platform.oais.tasks.pipeline_actions.create_retry_step.apply_async")
    def test_fts_job_status_success(self, create_retry_step):
        self.fts.job_status.return_value = {"job_state": "FINISHED"}
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.COMPLETED)
        self.assertFalse(
            PeriodicTask.objects.filter(name=self.periodic_task.name).exists()
        )
        create_retry_step.assert_not_called()

    @patch("oais_platform.oais.tasks.pipeline_actions.create_retry_step.apply_async")
    def test_fts_job_status_failed(self, create_retry_step):
        self.fts.job_status.return_value = {"job_state": "FAILED"}
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        create_retry_step.assert_called_once()

    @patch("oais_platform.oais.tasks.pipeline_actions.create_retry_step.apply_async")
    def test_fts_job_status_failed_multiple_times(self, create_retry_step):
        self.step.input_data = json.dumps({"retry_count": FTS_MAX_RETRY_COUNT})
        self.step.save()
        self.fts.job_status.return_value = {"job_state": "FAILED"}
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        create_retry_step.assert_not_called()
