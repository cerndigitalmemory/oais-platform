from unittest.mock import patch

from django.contrib.auth.models import Permission, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import ApiKey, Archive, Source, Status, Step, Steps
from oais_platform.oais.serializers import ArchiveSerializer


class PipelineTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")
        self.source = Source.objects.create(
            name="test", longname="Test", api_url="test.test/api", classname="Local"
        )
        self.creator_api_key = ApiKey.objects.create(
            user=self.creator, source=self.source, key="abcd1234"
        )
        self.other_user = User.objects.create_user("other", password="pw")

        self.archive = Archive.objects.create(
            recid="1", source=self.source.name, source_url="", creator=self.creator
        )

    @patch("oais_platform.oais.tasks.process.delay")
    def test_create_step_harvest(self, process_delay):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.HARVEST},
            format="json",
        )

        latest_step = Step.objects.latest("id")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.HARVEST)
        self.assertEqual(response.data["status"], Status.WAITING)
        process_delay.assert_called_once_with(
            self.archive.id,
            latest_step.id,
            self.creator_api_key.key,
            input_data=latest_step.output_data,
        )

    @patch("oais_platform.oais.tasks.validate.delay")
    def test_create_step_validate(self, validate_delay):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.VALIDATION},
            format="json",
        )

        latest_step = Step.objects.latest("id")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.VALIDATION)
        self.assertEqual(response.data["status"], Status.WAITING)
        validate_delay.assert_called_once_with(
            self.archive.id, latest_step.id, latest_step.output_data
        )

    @patch("oais_platform.oais.tasks.checksum.delay")
    def test_create_step_checksum(self, checksum_delay):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.CHECKSUM},
            format="json",
        )

        latest_step = Step.objects.latest("id")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.CHECKSUM)
        self.assertEqual(response.data["status"], Status.WAITING)
        checksum_delay.assert_called_once_with(
            self.archive.id, latest_step.id, latest_step.output_data
        )

    @patch("oais_platform.oais.tasks.archivematica.delay")
    def test_create_step_archivematica(self, am_delay):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.ARCHIVE},
            format="json",
        )

        latest_step = Step.objects.latest("id")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.ARCHIVE)
        self.assertEqual(response.data["status"], Status.WAITING)
        am_delay.assert_called_once_with(
            self.archive.id, latest_step.id, latest_step.output_data
        )

    def test_edit_manifests(self):
        self.client.force_authenticate(user=self.creator)

        self.assertEqual(self.archive.manifest, None)

        url = reverse("archives-save-manifest", args=[self.archive.id])
        response = self.client.post(
            url,
            {"manifest": {"test": "test"}},
            format="json",
        )

        self.archive.refresh_from_db()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.manifest, {"test": "test"})

    @patch("oais_platform.oais.tasks.extract_title.delay")
    def test_create_step_extract_title(self, extract_title_delay):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.EXTRACT_TITLE},
            format="json",
        )

        latest_step = Step.objects.latest("id")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.EXTRACT_TITLE)
        self.assertEqual(response.data["status"], Status.WAITING)
        extract_title_delay.assert_called_once_with(self.archive.id, latest_step.id)
