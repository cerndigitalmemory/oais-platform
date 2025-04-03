from django.contrib.auth.models import Permission, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Collection
from oais_platform.oais.serializers import ArchiveSerializer


class CollectionTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="view_archive_all")

        self.requester = User.objects.create_user("requester", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")
        self.superuser = User.objects.create_superuser("superuser", password="pw")

        self.archive1 = Archive.objects.create(
            recid="1", source="test_archive", requester=self.requester
        )

        self.serializer1 = ArchiveSerializer(self.archive1, many=False)

        self.collection = Collection.objects.create(
            title="test", internal=False, creator=self.superuser
        )
        self.collection.add_archive(self.archive1)

        self.job = Collection.objects.create(
            title="job", internal=True, creator=self.superuser
        )
        self.collection.add_archive(self.archive1)

    def test_collection_list_no_perms(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("tags-list")
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 0)

    def test_collection_list_with_perms(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()
        self.client.force_authenticate(user=self.other_user)

        url = reverse("tags-list")
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 2)

    def test_collection_list_internal_only(self):
        self.client.force_authenticate(user=self.superuser)

        url = reverse("tags-list")
        response = self.client.get(
            url,
            {"internal": "only"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["id"], self.job.id)

    def test_collection_list_not_internal(self):
        self.client.force_authenticate(user=self.superuser)

        url = reverse("tags-list")
        response = self.client.get(
            url,
            {"internal": "false"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["id"], self.collection.id)

    def test_collection_detail_no_perms(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("tags-detail", args=[self.collection.id])
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_collection_detail_with_perms(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()
        self.client.force_authenticate(user=self.other_user)

        url = reverse("tags-detail", args=[self.collection.id])
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.collection.id)
        self.assertEqual(len(response.data["archives_count"]), 1)

    def test_collection_create(self):
        """
        Creates a collection and checks if the collection has been created
        """
        self.client.force_authenticate(user=self.requester)

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
        self.client.force_authenticate(user=self.requester)

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
        self.assertEqual(Collection.objects.filter(creator=self.requester).count(), 0)

    def test_multiple_collections(self):
        """
        Creates multiple collections and checks if they are created
        """
        self.client.force_authenticate(user=self.requester)

        url = reverse("tags-create")
        check_url = reverse("tags-list")
        for i in range(10):
            result = self.client.post(
                url,
                {
                    "title": f"test_{i}",
                    "description": "test description",
                    "archives": None,
                },
                format="json",
            )
            self.assertEqual(result.status_code, status.HTTP_200_OK)

        results = self.client.get(check_url, format="json")

        self.assertEqual(results.status_code, status.HTTP_200_OK)
        # Check if there are 10 collections
        self.assertEqual(results.data["count"], 10)
        self.assertEqual(Collection.objects.filter(creator=self.requester).count(), 10)

    def test_archive_in_collection(self):
        """
        Creates a collection with an archive, then checks if the archive is there
        """
        self.client.force_authenticate(user=self.requester)

        url = reverse("tags-create")
        check_url = reverse("tags-list")

        self.client.post(
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

        self.assertEqual(results.status_code, status.HTTP_200_OK)
        self.assertEqual(results.data["count"], 1)
        self.assertEqual(data[0]["archive_count"], 1)

    def test_archive_add(self):
        """
        Creates a collection and adds an archive to it, then checks if the archive is there
        """
        self.client.force_authenticate(user=self.requester)

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

        # Archives before add
        self.assertEqual(results.data["count"], 1)
        self.assertEqual(data[0]["archive_count"], 0)

        response_collection_id = response.data["id"]
        add_archive = reverse("tags-add-arch", args=[response_collection_id])

        add_archive_response = self.client.post(
            add_archive, {"archives": [self.archive1.id]}, format="json"
        )
        self.assertEqual(add_archive_response.status_code, status.HTTP_200_OK)

        results2 = self.client.get(check_url, format="json")

        data = results2.data["results"]

        self.assertEqual(results2.status_code, status.HTTP_200_OK)
        self.assertEqual(results2.data["count"], 1)
        self.assertEqual(data[0]["archive_count"], 1)

    def test_archive_remove(self):
        """
        Creates a collection with an archive and then removes it, then checks if the archive is removed but the collection is there
        """
        self.client.force_authenticate(user=self.requester)

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
        # Check if there is one archive in the beginning
        self.assertEqual(results.status_code, status.HTTP_200_OK)
        self.assertEqual(results.data["count"], 1)
        self.assertEqual(data[0]["archive_count"], 1)

        # Remove the archive from the collection
        response_collection_id = response.data["id"]
        rm_archive = reverse("tags-remove-arch", args=[response_collection_id])

        rm_archive_response = self.client.post(
            rm_archive, {"archives": [self.serializer1.data]}, format="json"
        )
        self.assertEqual(rm_archive_response.status_code, status.HTTP_200_OK)

        results2 = self.client.get(check_url, format="json")
        data = results2.data["results"]
        # Check if collection is there and there is no archive
        self.assertEqual(results2.status_code, status.HTTP_200_OK)
        self.assertEqual(results2.data["count"], 1)
        self.assertEqual(data[0]["archive_count"], 0)

    def test_check_duplicate_create(self):
        """
        Creates one tag and then creates another one with the same name.
        The second creation must fail.
        """
        self.client.force_authenticate(user=self.requester)

        url = reverse("tags-create")
        response = self.client.post(
            url,
            {"title": "test", "description": "test description", "archives": None},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        url = reverse("tags-create")
        response = self.client.post(
            url,
            {"title": "test", "description": "test description", "archives": None},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_check_duplicate_update(self):
        """
        Creates two tags with different names.
        The second tag is edited and the name changes to match the first one.
        The renaming must fail
        """
        self.client.force_authenticate(user=self.requester)

        url = reverse("tags-create")
        response = self.client.post(
            url,
            {"title": "test", "description": "test description", "archives": None},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        url = reverse("tags-create")
        response = self.client.post(
            url,
            {"title": "test2", "description": "test description", "archives": None},
            format="json",
        )

        # Get the id of the created tag
        second_tag_id = response.data["id"]

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        url = reverse("tags-edit", args=[second_tag_id])
        response = self.client.post(
            url,
            {"title": "test", "description": "test description"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        response = self.client.post(
            url,
            {"title": "new test", "description": "new test description"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["title"], "new test")
        self.assertEqual(response.data["description"], "new test description")

    def test_return_only_archives_of_collection(self):
        """
        Creates a empty collection with no archives
        Tries to retrieve all archvies connected to the collection
        """
        self.client.force_authenticate(user=self.superuser)
        url = reverse("tags-archives", args=[self.collection.id])
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["id"], self.archive1.id)

    def test_empty_collection_archives_should_be_0(self):
        """
        Creates a empty collection with no archives
        Tries to retrieve all archvies connected to the collection
        """
        collection_without_archives = Collection.objects.create(
            title="test_without_archives", internal=False, creator=self.superuser
        )

        self.client.force_authenticate(user=self.superuser)
        url = reverse("tags-archives", args=[collection_without_archives.id])
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 0)
