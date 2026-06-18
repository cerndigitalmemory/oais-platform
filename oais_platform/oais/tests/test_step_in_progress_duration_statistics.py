from datetime import timedelta

from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName

TOTAL_STEPS = len(StepName.values)


class StepDurationStatisticsTests(APITestCase):
    def create_archive_with_steps(self, steps):
        archive = Archive.objects.create()
        for step_name, step_status, start_date in steps:
            Step.objects.create(
                step_name=step_name,
                status=step_status,
                archive=archive,
                start_date=start_date,
            )
        archive.save()
        return archive

    def setUp(self):
        self.url = reverse("step_duration_statistics")

    def get_avg_duration(self, data, step):
        return next(row["avg_duration"] for row in data if row["step"] == step)

    def test_step_duration_in_progress(self):
        self.create_archive_with_steps(
            [
                (
                    StepName.ARCHIVE,
                    Status.IN_PROGRESS,
                    timezone.now() - timedelta(hours=3),
                )
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        avg_duration = self.get_avg_duration(response.data, StepName.ARCHIVE)
        self.assertAlmostEqual(avg_duration, 3 * 3600, delta=60)

    def test_step_duration_average_multiple_steps(self):
        self.create_archive_with_steps(
            [
                (
                    StepName.ARCHIVE,
                    Status.IN_PROGRESS,
                    timezone.now() - timedelta(hours=2),
                )
            ]
        )
        self.create_archive_with_steps(
            [
                (
                    StepName.ARCHIVE,
                    Status.IN_PROGRESS,
                    timezone.now() - timedelta(hours=4),
                )
            ]
        )

        response = self.client.get(self.url, format="json")
        avg_duration = self.get_avg_duration(response.data, StepName.ARCHIVE)
        self.assertAlmostEqual(avg_duration, 3 * 3600, delta=60)

    def test_step_duration_excludes_not_in_progress(self):
        self.create_archive_with_steps(
            [(StepName.ARCHIVE, Status.COMPLETED, timezone.now() - timedelta(hours=3))]
        )
        self.create_archive_with_steps(
            [(StepName.ARCHIVE, Status.FAILED, timezone.now() - timedelta(hours=5))]
        )

        response = self.client.get(self.url, format="json")
        self.assertIsNone(self.get_avg_duration(response.data, StepName.ARCHIVE))

    def test_step_duration_no_in_progress_steps(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(all(row["avg_duration"] is None for row in response.data))

    def test_step_duration_one_row_per_step(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), TOTAL_STEPS)
        self.assertEqual({row["step"] for row in response.data}, set(StepName.values))

    def test_step_duration_uses_latest_attempt(self):
        self.create_archive_with_steps(
            [
                (StepName.ARCHIVE, Status.FAILED, timezone.now() - timedelta(hours=5)),
                (
                    StepName.ARCHIVE,
                    Status.IN_PROGRESS,
                    timezone.now() - timedelta(hours=1),
                ),
            ]
        )

        response = self.client.get(self.url, format="json")
        avg_duration = self.get_avg_duration(response.data, StepName.ARCHIVE)
        self.assertAlmostEqual(avg_duration, 1 * 3600, delta=60)
