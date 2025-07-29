from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps


class StepStatisticsEndpointTest(APITestCase):
    def setUp(self):
        self.url = reverse("step_statistics")

        harvested_archive = Archive.objects.create()
        preserved_archive = Archive.objects.create()
        tape_archive = Archive.objects.create()
        registry_archive = Archive.objects.create()
        tape_and_registry_archive = Archive.objects.create()
        step_data = {
            harvested_archive: [Steps.CHECKSUM],
            preserved_archive: [Steps.CHECKSUM, Steps.ARCHIVE],
            tape_archive: [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.PUSH_TO_CTA,
            ],
            registry_archive: [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.INVENIO_RDM_PUSH,
            ],
            tape_and_registry_archive: [
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

    def test_step_statistics(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "only_harvested_count": 1,
                "harvested_preserved_count": 1,
                "harvested_preserved_tape_count": 1,
                "harvested_preserved_registry_count": 1,
                "harvested_preserved_tape_registry_count": 1,
            },
        )

    def test_step_statistics_more_archives(self):
        tape_archive = Archive.objects.create()
        registry_archive = Archive.objects.create()
        tape_and_registry_archive = Archive.objects.create()
        step_data = {
            tape_archive: [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.PUSH_TO_CTA,
            ],
            registry_archive: [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.INVENIO_RDM_PUSH,
            ],
            tape_and_registry_archive: [
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

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "only_harvested_count": 1,
                "harvested_preserved_count": 1,
                "harvested_preserved_tape_count": 2,
                "harvested_preserved_registry_count": 2,
                "harvested_preserved_tape_registry_count": 2,
            },
        )
