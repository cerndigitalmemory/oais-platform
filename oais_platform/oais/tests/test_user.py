from django.contrib.auth.models import User
from django.urls import reverse
from parameterized import parameterized
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import ApiKey, Archive, Collection, Source


class UserTests(APITestCase):
    """
    Test cases for the User related endpoints.
    """

    def setUp(self):
        """
        Set up the test data.
        """
        self.superuser = User.objects.create_superuser("superuser", "", "pw")
        self.test_user = User.objects.create_user("test_user", password="pw")
        self.test_user2 = User.objects.create_user("test_user2", password="pw")

        self.super_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.superuser,
            approver=self.superuser,
            restricted=True,
        )

        self.test_archive = Archive.objects.create(
            recid="abc",
            source="test2",
            source_url="",
            requester=self.test_user,
            approver=self.superuser,
            restricted=True,
        )

        self.test_approved_archive = Archive.objects.create(
            recid="abc",
            source="test2",
            source_url="",
            requester=self.superuser,
            approver=self.test_user,
            restricted=True,
        )

        self.source = Source.objects.create(
            name="test",
            longname="Test",
            api_url="test.test/api",
            classname="Local",
        )

        self.internal_tag = Collection.objects.create(
            internal=True,
            creator=self.test_user,
        )

        self.regular_tag = Collection.objects.create(
            internal=False,
            creator=self.test_user,
        )

        self.superuser_tag = Collection.objects.create(
            internal=False,
            creator=self.superuser,
        )

    def _set_archive_to_staged(self, archive):
        """
        Set the archive to staged.
        """
        archive.approver = None
        archive.staged = True
        archive.save()

    def test_users_get_list_denied(self):
        """
        Test that users cannot get the list of users.
        """
        self.client.force_authenticate(user=self.test_user)
        url = reverse("users-list")
        response = self.client.get(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_users_get_list(self):
        """
        Test that superusers can get the list of users.
        """
        self.client.force_authenticate(user=self.superuser)
        url = reverse("users-list")
        response = self.client.get(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), User.objects.all().count())

    def test_users_get_user_denied(self):
        """
        Test that users cannot get other users' details.
        """
        self.client.force_authenticate(user=self.test_user)
        url = reverse("users-detail", kwargs={"pk": self.superuser.id})
        response = self.client.get(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_users_get_user_self(self):
        """
        Test that users can get their own details.
        """
        self.client.force_authenticate(user=self.test_user)
        url = reverse("users-detail", kwargs={"pk": self.test_user.id})
        response = self.client.get(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.test_user.id)

    def test_users_get_user(self):
        """
        Test that superusers can get other users' details.
        """
        self.client.force_authenticate(user=self.superuser)
        url = reverse("users-detail", kwargs={"pk": self.test_user2.id})
        response = self.client.get(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.test_user2.id)

    @parameterized.expand(
        [
            (
                {
                    "request_user": "superuser",
                    "requested_user": "superuser",
                },
                lambda self: [
                    self.super_archive.id,
                    self.test_archive.id,
                    self.test_approved_archive.id,
                ],
                status.HTTP_200_OK,
            ),
            (
                {
                    "request_user": "superuser",
                    "requested_user": "test_user",
                },
                lambda self: [self.test_archive.id, self.test_approved_archive.id],
                status.HTTP_200_OK,
            ),
            (
                {
                    "request_user": "superuser",
                    "requested_user": "test_user2",
                },
                lambda self: [],
                status.HTTP_200_OK,
            ),
            (
                {
                    "request_user": "test_user",
                    "requested_user": "test_user",
                },
                lambda self: [self.test_archive.id, self.test_approved_archive.id],
                status.HTTP_200_OK,
            ),
            (
                {
                    "request_user": "test_user",
                    "requested_user": "test_user2",
                },
                lambda self: [],
                status.HTTP_403_FORBIDDEN,
            ),
        ]
    )
    def test_users_get_archives(self, input, results, status_code):
        """
        Test getting archives for different users.
        """
        self.client.force_authenticate(
            user=User.objects.get(username=input["request_user"])
        )

        url = reverse(
            "users-archives",
            args=[User.objects.get(username=input["requested_user"]).id],
        )
        response = self.client.get(url, format="json")

        result_ids = results(self)
        self.assertEqual(response.status_code, status_code)
        if status_code == status.HTTP_200_OK:
            self.assertEqual(len(response.data["results"]), len(result_ids))
            for archive in response.data["results"]:
                self.assertTrue(archive["id"] in result_ids)

    def test_users_get_set_me(self):
        """
        Test getting and setting user details.
        """
        self.client.force_authenticate(user=self.test_user)

        url = reverse("users-me")
        user_response = self.client.get(url)
        self.assertEqual(user_response.status_code, status.HTTP_200_OK)
        data = user_response.data
        self.assertEqual(data["id"], self.test_user.id)
        self.assertEqual(data["username"], self.test_user.username)
        test_api_key = next(
            (item for item in data["api_key"] if item["source_id"] == self.source.id),
            None,
        )
        self.assertEqual(test_api_key["source"], self.source.longname)
        self.assertEqual(test_api_key["key"], None)

        response = self.client.post(
            url, {"source": self.source.id, "key": "testkey"}, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        apikey = ApiKey.objects.filter(user=self.test_user, source=self.source)
        self.assertEqual(apikey.count(), 1)
        apikey = apikey.first()
        self.assertEqual(apikey.key, "testkey")

    def test_users_get_set_me_override(self):
        """
        Test overriding user details.
        """
        ApiKey.objects.create(user=self.test_user, source=self.source, key="oldkey")
        self.client.force_authenticate(user=self.test_user)

        url = reverse("users-me")
        user_response = self.client.get(url)
        self.assertEqual(user_response.status_code, status.HTTP_200_OK)
        data = user_response.data
        self.assertEqual(data["id"], self.test_user.id)
        self.assertEqual(data["username"], self.test_user.username)
        test_api_key = next(
            (item for item in data["api_key"] if item["source_id"] == self.source.id),
            None,
        )
        self.assertEqual(test_api_key["source"], self.source.longname)
        self.assertEqual(test_api_key["key"], "oldkey")

        response = self.client.post(
            url, {"source": self.source.id, "key": "newkey"}, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        apikey = ApiKey.objects.filter(user=self.test_user, source=self.source)
        self.assertEqual(apikey.count(), 1)
        apikey = apikey.first()
        self.assertEqual(apikey.key, "newkey")

    def test_users_get_set_me_delete(self):
        """
        Test deleting user details.
        """
        ApiKey.objects.create(user=self.test_user, source=self.source, key="oldkey")
        self.client.force_authenticate(user=self.test_user)

        url = reverse("users-me")
        user_response = self.client.get(url)
        self.assertEqual(user_response.status_code, status.HTTP_200_OK)
        data = user_response.data
        self.assertEqual(data["id"], self.test_user.id)
        self.assertEqual(data["username"], self.test_user.username)
        test_api_key = next(
            (item for item in data["api_key"] if item["source_id"] == self.source.id),
            None,
        )
        self.assertEqual(test_api_key["source"], self.source.longname)
        self.assertEqual(test_api_key["key"], "oldkey")

        response = self.client.post(
            url, {"source": self.source.id, "key": None}, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        apikey = ApiKey.objects.filter(user=self.test_user, source=self.source)
        self.assertEqual(apikey.count(), 0)

    def test_users_get_tags(self):
        """
        Test getting user tags.
        """
        self.client.force_authenticate(user=self.test_user)

        url = reverse("users-me-tags")
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        tag = response.data[0]
        self.assertEqual(tag["id"], self.regular_tag.id)

    def test_users_get_add_staging_area(self):
        """
        Test adding records to the staging area.
        """
        self._set_archive_to_staged(self.super_archive)
        self._set_archive_to_staged(self.test_archive)
        self.client.force_authenticate(user=self.test_user)

        url = reverse("users-me-staging-area")
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)
        result = response.data["results"][0]
        self.assertEqual(result["id"], self.test_archive.id)
        self.assertEqual(result["duplicates"][0]["id"], self.test_approved_archive.id)

        url_add = reverse("users-me-stage")
        response = self.client.post(
            url_add,
            {
                "records": [
                    {
                        "recid": "ab",
                        "source": "testsource",
                        "source_url": "test_url",
                        "title": "staged title",
                    }
                ]
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 2)

    def test_users_get_staging_area_superuser(self):
        """
        Test getting the staging area for superusers.
        """
        self._set_archive_to_staged(self.super_archive)
        self._set_archive_to_staged(self.test_archive)
        self.client.force_authenticate(user=self.superuser)

        url = reverse("users-me-staging-area")
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 2)
        for result in response.data["results"]:
            self.assertEqual(
                result["id"] in [self.test_archive.id, self.super_archive.id], True
            )
