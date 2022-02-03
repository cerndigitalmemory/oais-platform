from django.contrib.auth.models import Permission, User
from django.urls import reverse
from oais_platform.oais.models import Archive, Collection
from rest_framework import status
from rest_framework.test import APITestCase
from oais_platform.oais.serializers import ArchiveSerializer


class CollectionTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")

        self.archive1 = Archive.objects.create(
            recid="1", source="test_archive", creator=self.creator
        )
        self.serializer1 = ArchiveSerializer(self.archive1, many=False)

    def check_collection(self):
        """
        Creates a collection and checks if the collection has been created
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("create_collection")
        response = self.client.post(
            url,
            {"title": "test", "description": "test description", "archives": None},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], 1)
        self.assertEqual(response.data["title"], "test")

    def check_remove_collection(self):
        """
        Creates a collection, checks if it has been created then deletes it and veryfies that there are no collections
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("create_collection")
        check_url = reverse("get-collections")

        response = self.client.post(
            url,
            {"title": "test", "description": "test description", "archives": None},
            format="json",
        )
        response_collection_id = response.data["id"]
        self.assertEqual(response_collection_id, 1)

        del_url = reverse("collections-delete", args=[response_collection_id])
        response2 = self.client.post(del_url, format="json")

        results = self.client.get(check_url, format="json")
        self.assertEqual(results.data["count"], 0)
        self.assertEqual(response2.status_code, status.HTTP_200_OK)
        self.assertEqual(response2.data, None)

    def check_multiple_collections(self):
        """
        Creates multiple collections and checks if they are created
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("create_collection")
        check_url = reverse("get-collections")
        for i in range(len(10)):
            response = self.client.post(
                url,
                {
                    "title": f"test_{i}",
                    "description": "test description",
                    "archives": None,
                },
                format="json",
            )

        results = self.client.get(check_url, format="json")

        self.assertEqual(results.status_code, status.HTTP_200_OK)
        # Check if there are 10 collections
        self.assertEqual(results.data["count"], 10)

    def check_archive_in_collection(self):
        """
        Creates a collection and adds an archive to it, then checks if the archive is there
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("create_collection")
        check_url = reverse("get-collections")

        response1 = self.client.post(
            url,
            {
                "title": "test",
                "description": "test description",
                "archives": [self.serializer1.data["id"]],
            },
            format="json",
        )
        results = self.client.get(check_url, format="json")

        data = results.data["results"]
        archives = data[0]["archives"]

        self.assertEqual(results.status_code, status.HTTP_200_OK)
        self.assertEqual(results.data["count"], 1)
        self.assertEqual(len(archives), 1)
        self.assertEqual(archives[0]["id"], 1)
        self.assertEqual(archives[0]["source"], "test_archive")
