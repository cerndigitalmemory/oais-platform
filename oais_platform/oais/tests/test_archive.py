from django.contrib.auth.models import Permission, User
from django.db import IntegrityError
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Resource, Step


class ArchiveTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")

        self.private_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            creator=self.creator,
            restricted=True,
        )

        self.public_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            creator=self.creator,
            restricted=False,
        )

    def test_archive_list_creator_public(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-list")
        response = self.client.get(url, {"filter": "public"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)

    def test_archive_list_creator_private(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-list")
        response = self.client.get(url, {"filter": "private"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)

    def test_archive_list_creator_owned(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-list")
        response = self.client.get(url, {"filter": "owned"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 2)

    def test_archive_list_other_user_private(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, {"filter": "private"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 0)

    def test_archive_list_other_user_public(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, {"filter": "public"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)

    def test_archive_list_other_user_owned(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, {"filter": "owned"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 0)

    def test_archive_list_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, {"filter": "private"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 2)

    def test_archive_details_creator(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(url, {"filter": "owned"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_archive_details_creator_with_perm(self):
        self.creator.user_permissions.add(self.permission)
        self.creator.save()

        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(url, {"filter": "owned"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_archive_details_other_user(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(url, {"filter": "private"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_archive_details_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(url, {"filter": "private"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_get_archive_details(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_get_steps(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-steps", args=[self.private_archive.id])
        response = self.client.get(
            url,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        self.step1 = Step.objects.create(archive=self.private_archive, name=0)
        self.step2 = Step.objects.create(archive=self.private_archive, name=0)

        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

    def test_record_check(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("check_archived_records")
        response = self.client.post(
            url, {"recordList": [{"recid": "1", "source": "test"}]}, format="json"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]["archives"]), 2)
        self.assertEqual(response.data[0]["archives"][0]["recid"], "1")
        self.assertEqual(response.data[0]["archives"][0]["source"], "test")

    def test_resource_created(self):
        self.assertEqual(Resource.objects.all().count(), 1)
        # This recid already exists. Therefore, the number of objects should not increase
        Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            creator=self.creator,
            restricted=True,
        )
        self.assertEqual(Resource.objects.all().count(), 1)
        Archive.objects.create(
            recid="2",
            source="test",
            source_url="",
            creator=self.creator,
            restricted=True,
        )
        self.assertEqual(Resource.objects.all().count(), 2)

        with self.assertRaises(IntegrityError):
            Resource.objects.create(recid="2", source="test")
