from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName


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

    def test_step_status_statistics(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertCountEqual(
            response.data,
            [
                {"step": StepName.HARVEST, "status": "COMPLETED", "count": 2},
                {"step": StepName.HARVEST, "status": "IN_PROGRESS", "count": 1},
                {"step": StepName.ARCHIVE, "status": "COMPLETED", "count": 1},
                {"step": StepName.ARCHIVE, "status": "FAILED", "count": 1},
                {"step": StepName.PUSH_TO_CTA, "status": "COMPLETED", "count": 1},
            ],
        )

    def test_step_status_statistics_empty_database(self):
        Archive.objects.all().delete()
        Step.objects.all().delete()

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

    def test_step_status_statistics_omits_zero_counts(self):
        Archive.objects.all().delete()
        Step.objects.all().delete()
        self.create_archive_with_steps([(StepName.HARVEST, Status.COMPLETED)])

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            [{"step": StepName.HARVEST, "status": "COMPLETED", "count": 1}],
        )
