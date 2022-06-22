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

        self.collection = Collection.objects.create(title="test", internal=False)
        self.collection.add_archive(self.archive1)

    def test_collection(self):
        """
        Creates a collection and checks if the collection has been created
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("tags-create")
        response = self.client.post(
            url,
            {"title": "test", "description": "test description", "archives": None},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["title"], "test")
        self.assertEqual(response.data["description"], "test description")

    def test_remove_collection(self):
        """
        Creates a collection, checks if it has been created then deletes it and veryfies that there are no collections
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("tags-create")
        check_url = reverse("tags-list")

        response = self.client.post(
            url,
            {"title": "test", "description": "test description", "archives": None},
            format="json",
        )
        response_collection_id = response.data["id"]

        del_url = reverse("tags-delete", args=[response_collection_id])
        response2 = self.client.post(del_url, format="json")

        results = self.client.get(check_url, format="json")
        self.assertEqual(results.data["count"], 0)
        self.assertEqual(response2.status_code, status.HTTP_200_OK)
        self.assertEqual(response2.data, None)

    def test_multiple_collections(self):
        """
        Creates multiple collections and checks if they are created
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("tags-create")
        check_url = reverse("tags-list")
        for i in range(10):
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

    def test_archive_in_collection(self):
        """
        Creates a collection and adds an archive to it, then checks if the archive is there
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("tags-create")
        check_url = reverse("tags-list")

        response1 = self.client.post(
            url,
            {
                "title": "test",
                "description": "test description",
                "archives": [self.archive1.id],
            },
            format="json",
        )
        results = self.client.get(check_url, format="json")

        data = results.data["results"]
        archives = data[0]["archives"]

        self.assertEqual(results.status_code, status.HTTP_200_OK)
        self.assertEqual(results.data["count"], 1)
        self.assertEqual(len(archives), 1)
        self.assertEqual(archives[0]["id"], self.archive1.id)
        self.assertEqual(archives[0]["source"], "test_archive")

    def test_archive_add(self):
        """
        Creates a collection and adds an archive to it, then checks if the archive is there
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("tags-create")
        check_url = reverse("tags-list")

        # Creates a collection
        response = self.client.post(
            url,
            {
                "title": "test",
                "description": "test description",
                "archives": None,
            },
            format="json",
        )

        results = self.client.get(check_url, format="json")

        data = results.data["results"]
        archives = data[0]["archives"]
        # Archives before add
        self.assertEqual(results.data["count"], 1)
        self.assertEqual(len(archives), 0)

        response_collection_id = response.data["id"]
        add_archive = reverse("tags-add-arch", args=[response_collection_id])

        add_archive_response = self.client.post(
            add_archive, {"archives": [self.archive1.id]}, format="json"
        )

        results2 = self.client.get(check_url, format="json")

        data = results2.data["results"]
        archives = data[0]["archives"]

        self.assertEqual(results2.status_code, status.HTTP_200_OK)
        self.assertEqual(results2.data["count"], 1)
        self.assertEqual(len(archives), 1)

    def test_archive_remove(self):
        """
        Creates a collection with an archive and then removes it, then checks if the archive is removed but the collection is there
        """
        self.client.force_authenticate(user=self.creator)

        url = reverse("tags-create")
        check_url = reverse("tags-list")

        # Creates a collection with an archive
        response = self.client.post(
            url,
            {
                "title": "test",
                "description": "test description",
                "archives": [self.archive1.id],
            },
            format="json",
        )

        # Get all the collections
        results = self.client.get(check_url, format="json")
        data = results.data["results"]
        archives = data[0]["archives"]
        # Check if there is one archive in the beginning
        self.assertEqual(results.status_code, status.HTTP_200_OK)
        self.assertEqual(results.data["count"], 1)
        self.assertEqual(len(archives), 1)

        # Remove the archive from the collection
        response_collection_id = response.data["id"]
        rm_archive = reverse("tags-remove-arch", args=[response_collection_id])

        rm_archive_response = self.client.post(
            rm_archive, {"archives": [self.archive1.id]}, format="json"
        )

        results2 = self.client.get(check_url, format="json")

        data = results2.data["results"]
        archives = data[0]["archives"]

        # Check if collection is there and there is no archive
        self.assertEqual(results2.status_code, status.HTTP_200_OK)
        self.assertEqual(results2.data["count"], 1)
        self.assertEqual(len(archives), 0)

    def test_collection_list_other_user(self):
        self.client.force_authenticate(user=self.other_user)

        check_url = reverse("tags-list")
        results = self.client.get(check_url, format="json")

        self.assertEqual(results.status_code, status.HTTP_200_OK)
        self.assertEqual(results.data["count"], 0)

    def test_collection_list_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        check_url = reverse("tags-list")
        results = self.client.get(check_url, format="json")

        self.assertEqual(results.status_code, status.HTTP_200_OK)
        self.assertEqual(results.data["count"], 1)
