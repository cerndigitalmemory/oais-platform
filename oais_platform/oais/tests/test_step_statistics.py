from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps


class StepStatisticsEndpointTest(APITestCase):
    def create_archive_with_steps(self, steps_to_complete):
        archive = Archive.objects.create()
        for step_name in steps_to_complete:
            Step.objects.create(
                name=step_name, status=Status.COMPLETED, archive=archive
            )
        archive.save()
        return archive

    def setUp(self):
        self.url = reverse("step_statistics")
        self.create_archive_with_steps([])
        self.create_archive_with_steps([Steps.CHECKSUM])
        self.create_archive_with_steps([Steps.CHECKSUM, Steps.ARCHIVE])
        self.create_archive_with_steps(
            [Steps.CHECKSUM, Steps.ARCHIVE, Steps.PUSH_TO_CTA]
        )
        self.create_archive_with_steps(
            [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.INVENIO_RDM_PUSH,
            ]
        )
        self.create_archive_with_steps(
            [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.PUSH_TO_CTA,
                Steps.INVENIO_RDM_PUSH,
            ]
        )

    def test_step_statistics(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "staged_count": 1,
                "only_harvested_count": 1,
                "harvested_preserved_count": 1,
                "harvested_preserved_tape_count": 1,
                "harvested_preserved_registry_count": 1,
                "harvested_preserved_tape_registry_count": 1,
                "others_count": 0,
            },
        )

    def test_step_statistics_more_archives(self):
        self.create_archive_with_steps(
            [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.PUSH_TO_CTA,
            ]
        )
        self.create_archive_with_steps(
            [
                Steps.CHECKSUM,
                Steps.ARCHIVE,
                Steps.INVENIO_RDM_PUSH,
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "staged_count": 1,
                "only_harvested_count": 1,
                "harvested_preserved_count": 1,
                "harvested_preserved_tape_count": 2,
                "harvested_preserved_registry_count": 2,
                "harvested_preserved_tape_registry_count": 1,
                "others_count": 0,
            },
        )

    def test_step_statistics_empty_database(self):
        Archive.objects.all().delete()
        Step.objects.all().delete()

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "staged_count": 0,
                "only_harvested_count": 0,
                "harvested_preserved_count": 0,
                "harvested_preserved_tape_count": 0,
                "harvested_preserved_registry_count": 0,
                "harvested_preserved_tape_registry_count": 0,
                "others_count": 0,
            },
        )

    def test_step_statistics_mixed_status_steps(self):
        archive_mixed_status = self.create_archive_with_steps(
            [Steps.CHECKSUM, Steps.ARCHIVE]
        )
        Step.objects.create(
            name=Steps.PUSH_TO_CTA,
            status=Status.IN_PROGRESS,
            archive=archive_mixed_status,
        )
        Step.objects.create(
            name=Steps.INVENIO_RDM_PUSH,
            status=Status.FAILED,
            archive=archive_mixed_status,
        )
        archive_mixed_status.save()

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "staged_count": 1,
                "only_harvested_count": 1,
                "harvested_preserved_count": 2,
                "harvested_preserved_tape_count": 1,
                "harvested_preserved_registry_count": 1,
                "harvested_preserved_tape_registry_count": 1,
                "others_count": 0,
            },
        )
