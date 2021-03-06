from django.contrib.auth.models import Permission, User
from django.urls import reverse
from oais_platform.oais.models import Archive, Record
from rest_framework import status
from rest_framework.test import APITestCase


class ArchiveTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(
            codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")

        self.record = Record.objects.create(recid="1", source="test", url="")
        self.archive = Archive.objects.create(
            record=self.record, creator=self.creator)

    def test_archive_list_creator(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archive-list")
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)

    def test_archive_list_creator_with_perm(self):
        self.creator.user_permissions.add(self.permission)
        self.creator.save()

        self.client.force_authenticate(user=self.creator)

        url = reverse("archive-list")
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)

    def test_archive_list_other_user(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archive-list")
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 0)

    def test_archive_list_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archive-list")
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)

    def test_archive_details_creator(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archive-detail", args=[self.archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.archive.id)

    def test_archive_details_creator_with_perm(self):
        self.creator.user_permissions.add(self.permission)
        self.creator.save()

        self.client.force_authenticate(user=self.creator)

        url = reverse("archive-detail", args=[self.archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.archive.id)

    def test_archive_details_other_user(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archive-detail", args=[self.archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_archive_details_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archive-detail", args=[self.archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.archive.id)
