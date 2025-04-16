from unittest.mock import MagicMock

from django.apps import apps
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks import check_fts_job_status


class CheckFTSJobStatusTests(APITestCase):
    def setUp(self):
        self.app_config = apps.get_app_config("oais")
        self.fts = MagicMock()
        self.app_config.fts = self.fts

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

    def test_fts_job_status_success(self):
        self.fts.job_status.return_value = {"job_state": "FINISHED"}
        print(f"Status before task: {Step.objects.get(id=self.step.id).status}")
        print(f"Number of steps before task: {Step.objects.all().count()}")
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        self.step.refresh_from_db()
        print(f"Status after task: {Step.objects.get(id=self.step.id).status}")
        print(f"Number of steps after task: {Step.objects.all().count()}")
        self.assertEqual(self.step.status, Status.COMPLETED)
        self.assertEqual(
            PeriodicTask.objects.filter(name=self.periodic_task.name).first(), None
        )
        self.assertEqual(Step.objects.exclude(status=Status.COMPLETED).exists(), False)

    def test_fts_job_status_failed(self):
        self.fts.job_status.return_value = {"job_state": "FAILED"}
        print(f"Status before task: {Step.objects.get(id=self.step.id).status}")
        print(f"Number of steps before task: {Step.objects.all().count()}")
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        self.step.refresh_from_db()
        print(f"Status after task: {Step.objects.get(id=self.step.id).status}")
        print(f"Number of steps after task: {Step.objects.all().count()}")
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(Step.objects.exclude(status=Status.FAILED).exists(), True)

    def test_fts_job_statusfailed_multiple_times(self):
        print(f"Status before task: {Step.objects.get(id=self.step.id).status}")
        print(f"Number of steps before task: {Step.objects.all().count()}")
        Step.objects.create(
            archive=self.archive, name=Steps.PUSH_TO_CTA, status=Status.FAILED
        )
        self.fts.job_status.return_value = {"job_state": "FAILED"}
        check_fts_job_status.apply(args=[self.archive.id, self.step.id, "test_job_id"])
        self.step.refresh_from_db()
        print(f"Status after task: {Step.objects.get(id=self.step.id).status}")
        print(f"Number of steps after task: {Step.objects.all().count()}")
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(Step.objects.exclude(status=Status.FAILED).exists(), False)
