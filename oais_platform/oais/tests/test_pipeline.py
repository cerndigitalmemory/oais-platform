from django.contrib.auth.models import Permission, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.serializers import ArchiveSerializer


class PipelineTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")

        self.archive = Archive.objects.create(
            recid="1", source="test", source_url="", creator=self.creator
        )

    def test_create_step_harvest(self):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.HARVEST},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.HARVEST)
        self.assertEqual(response.data["status"], Status.WAITING)

    def test_create_step_validate(self):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.VALIDATION},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.VALIDATION)
        self.assertEqual(response.data["status"], Status.WAITING)

    def test_create_step_checksum(self):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.CHECKSUM},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.CHECKSUM)
        self.assertEqual(response.data["status"], Status.WAITING)

    def test_create_step_archivematica(self):
        self.client.force_authenticate(user=self.creator)

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        url = reverse("archives-next-step", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "next_step": Steps.ARCHIVE},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)
        self.assertEqual(response.data["name"], Steps.ARCHIVE)
        self.assertEqual(response.data["status"], Status.WAITING)

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
