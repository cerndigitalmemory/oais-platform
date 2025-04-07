from unittest.mock import patch

from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks import check_fts_job_status


class CheckFTSJobStatusTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create()
        self.step = Step.objects.create(archive=self.archive, name=Steps.PUSH_TO_CTA)
        self.step.set_output_data({"artifact": {"artifact_name": "FTS Job"}})
        schedule, _ = IntervalSchedule.objects.get_or_create(
            every=1, period=IntervalSchedule.HOURS
        )
        self.periodic_task = PeriodicTask.objects.create(
            interval=schedule,
            name=f"FTS job status for step: {self.step.id}",
            task="check_fts_job_status",
        )

    @patch("oais_platform.oais.fts.FTS.job_status")
    def test_fts_job_status_success(self, job_status):
        job_status.return_value = {"job_state": "FINISHED"}
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.COMPLETED)
        self.assertEqual(
            PeriodicTask.objects.filter(name=self.periodic_task.name).first(), None
        )
        self.assertEqual(Step.objects.exclude(status=Status.COMPLETED).exists(), False)

    @patch("oais_platform.oais.fts.FTS.job_status")
    def test_fts_job_status_failed(self, job_status):
        job_status.return_value = {"job_state": "FAILED"}
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(Step.objects.exclude(status=Status.FAILED).exists(), True)

    @patch("oais_platform.oais.fts.FTS.job_status")
    def test_fts_job_statusfailed_multiple_times(self, job_status):
        Step.objects.create(
            archive=self.archive, name=Steps.PUSH_TO_CTA, status=Status.FAILED
        )
        job_status.return_value = {"job_state": "FAILED"}
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(Step.objects.exclude(status=Status.FAILED).exists(), False)
