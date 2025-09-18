from unittest import skip

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName


class StepStatisticsEndpointTest(APITestCase):
    def create_archive_with_steps(self, steps_to_complete):
        archive = Archive.objects.create()
        for step_name in steps_to_complete:
            Step.objects.create(
                step_name=step_name, status=Status.COMPLETED, archive=archive
            )
        archive.save()
        return archive

    def setUp(self):
        self.url = reverse("step_statistics")

        staged_archive = self.create_archive_with_steps([])
        staged_archive.staged = True
        staged_archive.save()

        self.create_archive_with_steps([Steps.CHECKSUM])
        self.create_archive_with_steps([Steps.CHECKSUM, Steps.ARCHIVE])
        self.create_archive_with_steps(
            [StepName.CHECKSUM, StepName.ARCHIVE, StepName.PUSH_TO_CTA]
        )
        self.create_archive_with_steps(
            [
                StepName.CHECKSUM,
                StepName.ARCHIVE,
                StepName.INVENIO_RDM_PUSH,
            ]
        )
        self.create_archive_with_steps(
            [
                StepName.CHECKSUM,
                StepName.ARCHIVE,
                StepName.PUSH_TO_CTA,
                StepName.INVENIO_RDM_PUSH,
            ]
        )

    @skip("Temporarily skipped")
    def test_step_statistics(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "staged_count": 1,
                "harvested_count": 1,
                "harvested_preserved_count": 1,
                "harvested_preserved_tape_count": 1,
                "harvested_preserved_registry_count": 1,
                "harvested_preserved_tape_registry_count": 1,
                "others_count": 0,
            },
        )

    @skip("Temporarily skipped")
    def test_step_statistics_more_archives(self):
        self.create_archive_with_steps(
            [
                StepName.CHECKSUM,
                StepName.ARCHIVE,
                StepName.PUSH_TO_CTA,
            ]
        )
        self.create_archive_with_steps(
            [
                StepName.CHECKSUM,
                StepName.ARCHIVE,
                StepName.INVENIO_RDM_PUSH,
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "staged_count": 1,
                "harvested_count": 1,
                "harvested_preserved_count": 1,
                "harvested_preserved_tape_count": 2,
                "harvested_preserved_registry_count": 2,
                "harvested_preserved_tape_registry_count": 1,
                "others_count": 0,
            },
        )

    @skip("Temporarily skipped")
    def test_step_statistics_empty_database(self):
        Archive.objects.all().delete()
        Step.objects.all().delete()

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "staged_count": 0,
                "harvested_count": 0,
                "harvested_preserved_count": 0,
                "harvested_preserved_tape_count": 0,
                "harvested_preserved_registry_count": 0,
                "harvested_preserved_tape_registry_count": 0,
                "others_count": 0,
            },
        )

    @skip("Temporarily skipped")
    def test_step_statistics_mixed_status_steps(self):
        archive_mixed_status = self.create_archive_with_steps(
            [StepName.CHECKSUM, StepName.ARCHIVE]
        )
        Step.objects.create(
            step_name=StepName.PUSH_TO_CTA,
            status=Status.IN_PROGRESS,
            archive=archive_mixed_status,
        )
        Step.objects.create(
            step_name=StepName.INVENIO_RDM_PUSH,
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
                "harvested_count": 1,
                "harvested_preserved_count": 2,
                "harvested_preserved_tape_count": 1,
                "harvested_preserved_registry_count": 1,
                "harvested_preserved_tape_registry_count": 1,
                "others_count": 0,
            },
        )

    def test_step_statistics_others_count(self):
        self.create_archive_with_steps([Steps.CHECKSUM, Steps.PUSH_TO_CTA])
        self.create_archive_with_steps([Steps.INVENIO_RDM_PUSH])

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            {
                "staged_count": 1,
                "harvested_count": 1,
                "harvested_preserved_count": 1,
                "harvested_preserved_tape_count": 1,
                "harvested_preserved_registry_count": 1,
                "harvested_preserved_tape_registry_count": 1,
                "others_count": 2,
            },
        )
