from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps


class StatisticsEndpointTest(APITestCase):
    def setUp(self):
        self.url = reverse("statistics")

        self.harvested_archive = Archive.objects.create()
        self.preserved_archive = Archive.objects.create()
        self.pushed_archive = Archive.objects.create()
        step_data = {
            self.harvested_archive: [Steps.CHECKSUM],
            self.preserved_archive: [Steps.CHECKSUM, Steps.ARCHIVE],
            self.pushed_archive: [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.PUSH_TO_CTA,
                Steps.INVENIO_RDM_PUSH,
            ],
        }
        for archive, steps in step_data.items():
            for step in steps:
                Step.objects.create(name=step, status=Status.COMPLETED, archive=archive)
            archive.save()

    def test_statistics(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["harvested_count"], 3)
        self.assertEqual(response.data["preserved_count"], 2)
        self.assertEqual(response.data["pushed_to_tape_count"], 1)
        self.assertEqual(response.data["pushed_to_registry_count"], 1)

    def test_statistics_multiple_pushes(self):
        for step in (Steps.INVENIO_RDM_PUSH, Steps.PUSH_TO_CTA):
            Step.objects.create(
                name=step, status=Status.COMPLETED, archive=self.pushed_archive
            )
        self.pushed_archive.save()

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["harvested_count"], 3)
        self.assertEqual(response.data["preserved_count"], 2)
        self.assertEqual(response.data["pushed_to_tape_count"], 1)
        self.assertEqual(response.data["pushed_to_registry_count"], 1)
