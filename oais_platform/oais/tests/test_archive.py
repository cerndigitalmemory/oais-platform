from unittest import skip

from django.contrib.auth.models import Permission, User
from django.db import IntegrityError
from django.urls import reverse
from parameterized import parameterized
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, ArchiveState, Resource, Step


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

        self.public_archives = []
        resources = [
            ["1", "test", "test source 1"],
            ["7234", "source_1", "archive test 1"],
            ["3445", "source_2", "archive test 2"],
        ]
        for r in resources:
            archive = Archive.objects.create(
                recid=r[0],
                source=r[1],
                source_url="",
                creator=self.creator,
                restricted=False,
                title=r[2],
            )
            self.public_archives.append(archive)

    @skip("GET public Archives operation is unsupported")
    def test_archive_list_creator_public(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-list")
        response = self.client.get(url, {"access": "public"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)

    @parameterized.expand(
        [
            ({"access": "private"}, {"status": status.HTTP_200_OK, "size": 1}),
            ({"access": "owned"}, {"status": status.HTTP_200_OK, "size": 4}),
        ]
    )
    def test_archive_list_creator(self, access, output):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-list")
        response = self.client.get(url, access, format="json")

        self.assertEqual(response.status_code, output["status"])
        self.assertEqual(len(response.data["results"]), output["size"])

    @skip("GET public Archives operation is unsupported")
    def test_archive_list_other_user_public(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, {"access": "public"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)

    @parameterized.expand(
        [
            ({"access": "private"}, {"status": status.HTTP_200_OK, "size": 0}),
            ({"access": "owned"}, {"status": status.HTTP_200_OK, "size": 0}),
        ]
    )
    def test_archive_list_other_user(self, access, output):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, access, format="json")

        self.assertEqual(response.status_code, output["status"])
        self.assertEqual(len(response.data["results"]), output["size"])

    def test_archive_list_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, {"access": "private"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 4)

    @parameterized.expand(
        [
            (
                {"access": "all", "filters": {"source": "test", "query": "1"}},
                {"status": status.HTTP_200_OK, "size": 2},
            ),
            (
                {"access": "all", "filters": {"query": "723"}},
                {"status": status.HTTP_200_OK, "size": 1},
            ),
            (
                {"access": "all", "filters": {"query": "archive"}},
                {"status": status.HTTP_200_OK, "size": 2},
            ),
            (
                {"access": "all", "filters": {"source": "test2", "query": "1"}},
                {"status": status.HTTP_200_OK, "size": 0},
            ),
            ({"access": "all"}, {"status": status.HTTP_400_BAD_REQUEST, "size": 0}),
        ]
    )
    def test_archives_filtered(self, data, output):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-filter")
        response = self.client.post(url, data, format="json")
        self.assertEqual(response.status_code, output["status"])

        if response.status_code == status.HTTP_200_OK:
            self.assertEqual(len(response.data["results"]), output["size"])

    def test_archive_details_creator(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(url, {"access": "owned"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_archive_details_creator_with_perm(self):
        self.creator.user_permissions.add(self.permission)
        self.creator.save()

        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(url, {"access": "owned"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_archive_details_other_user(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(url, {"access": "private"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_archive_details_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-sgl-details", args=[self.private_archive.id])
        response = self.client.get(url, {"access": "private"}, format="json")

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

    def test_record_check_none(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("check_archived_records")
        response = self.client.post(
            url, {"recordList": [{"recid": "1", "source": "test"}]}, format="json"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]["archives"]), 0)

    def test_record_check(self):
        self.client.force_authenticate(user=self.creator)
        Step.objects.create(archive=self.private_archive, name=5, status=4)
        Step.objects.create(archive=self.public_archives[0], name=5, status=4)

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
        self.assertEqual(Resource.objects.all().count(), 3)
        # This recid already exists. Therefore, the number of objects should not increase
        Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            creator=self.creator,
            restricted=True,
        )
        self.assertEqual(Resource.objects.all().count(), 3)
        Archive.objects.create(
            recid="2",
            source="test",
            source_url="",
            creator=self.creator,
            restricted=True,
        )
        self.assertEqual(Resource.objects.all().count(), 4)

        with self.assertRaises(IntegrityError):
            Resource.objects.create(recid="2", source="test")

    def test_get_archives_sources(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-sources")
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(list(response.data), ["source_1", "source_2", "test"])
