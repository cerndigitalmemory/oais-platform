from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName

TOTAL_COMBINATIONS = len(StepName.values) * len(Status.values)


class StepStatusStatisticsEndpointTest(APITestCase):
    def create_archive_with_steps(self, steps):
        archive = Archive.objects.create()
        for step_name, step_status in steps:
            Step.objects.create(
                step_name=step_name, status=step_status, archive=archive
            )
        archive.save()
        return archive

    def setUp(self):
        self.url = reverse("step_status_statistics")

        self.create_archive_with_steps(
            [
                (StepName.HARVEST, Status.COMPLETED),
                (StepName.ARCHIVE, Status.COMPLETED),
                (StepName.PUSH_TO_CTA, Status.COMPLETED),
            ]
        )
        self.create_archive_with_steps(
            [
                (StepName.HARVEST, Status.COMPLETED),
                (StepName.ARCHIVE, Status.FAILED),
            ]
        )
        self.create_archive_with_steps(
            [
                (StepName.HARVEST, Status.IN_PROGRESS),
            ]
        )

    def get_count(self, data, step, step_status):
        return next(
            r["count"] for r in data if r["step"] == step and r["status"] == step_status
        )

    def test_step_status_statistics(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), TOTAL_COMBINATIONS)
        self.assertEqual(
            self.get_count(response.data, StepName.HARVEST, "COMPLETED"), 2
        )
        self.assertEqual(
            self.get_count(response.data, StepName.HARVEST, "IN_PROGRESS"), 1
        )
        self.assertEqual(self.get_count(response.data, StepName.HARVEST, "FAILED"), 0)
        self.assertEqual(
            self.get_count(response.data, StepName.ARCHIVE, "COMPLETED"), 1
        )
        self.assertEqual(self.get_count(response.data, StepName.ARCHIVE, "FAILED"), 1)
        self.assertEqual(
            self.get_count(response.data, StepName.PUSH_TO_CTA, "COMPLETED"), 1
        )
        self.assertEqual(
            self.get_count(response.data, StepName.PUSH_TO_CTA, "FAILED"), 0
        )

    def test_step_status_statistics_empty_database(self):
        Archive.objects.all().delete()
        Step.objects.all().delete()

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), TOTAL_COMBINATIONS)
        self.assertTrue(all(r["count"] == 0 for r in response.data))

    def test_step_status_statistics_includes_zero_counts(self):
        Archive.objects.all().delete()
        Step.objects.all().delete()
        self.create_archive_with_steps([(StepName.HARVEST, Status.COMPLETED)])

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), TOTAL_COMBINATIONS)
        self.assertEqual(
            self.get_count(response.data, StepName.HARVEST, "COMPLETED"), 1
        )
        self.assertEqual(self.get_count(response.data, StepName.HARVEST, "FAILED"), 0)
        self.assertEqual(
            self.get_count(response.data, StepName.ARCHIVE, "COMPLETED"), 0
        )
