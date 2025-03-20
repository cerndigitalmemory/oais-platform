from django.contrib.auth.models import Permission
from django.urls import reverse
from rest_framework.test import APITestCase
from rest_framework import status
from oais_platform.oais.models import Archive, ArchiveState, Step, Steps, Status


class StatisticsEndpointTest(APITestCase):
    def setUp(self):
        archives = [Archive.objects.create() for _ in range(3)]

        step_data = [
            (Steps.CHECKSUM, archives[0]),
            (Steps.CHECKSUM, archives[1]),
            (Steps.CHECKSUM, archives[2]),
            (Steps.ARCHIVE, archives[0]),
            (Steps.INVENIO_RDM_PUSH, archives[0]),
            (Steps.PUSH_TO_CTA, archives[0]),
        ]

        for name, archive in step_data:
            Step.objects.create(name=name, status=Status.COMPLETED, archive=archive)

        for archive in archives:
            archive.save()

    def test_statistics_endpoint(self):
        url = reverse("statistics")
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["harvested_count"], 3)
        self.assertEqual(response.data["preserved_count"], 1)
        self.assertEqual(response.data["pushed_to_tape_count"], 1)
        self.assertEqual(response.data["pushed_to_registry_count"], 1)
