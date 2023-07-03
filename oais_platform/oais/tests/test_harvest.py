from unittest import mock
from unittest.mock import patch

from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tasks import process
from oais_platform.oais.tests.utils import TestSource


class HarvestTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)

    def test_wrong_source(self):
        # Try to trigger an harvesting of an Archive with an invalid source id
        url = reverse("archives-create", args=["1", "a_bad_source"])
        response = self.client.post(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        # No archives and not steps should've been created
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Step.objects.count(), 0)

    @patch("oais_platform.oais.views.get_source")
    def test_harvest(self, get_source):
        # Make up a test "Source"
        source = TestSource()
        get_source.return_value = source

        url = reverse("archives-create", args=["1", "test"])
        response = self.client.post(url, format="json", follow=True)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertNotEqual(len(get_source.call_args_list), 0)
        for args in get_source.call_args_list:
            self.assertEqual(args, mock.call("test"))

        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 0)

        archive = Archive.objects.all()[0]

        url = reverse("harvest", args=[archive.id])
        response = self.client.post(url, format="json", follow=True)

        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Step.objects.count(), 1)

        step = Step.objects.all()[0]

        self.assertEqual(step.archive, archive)
        self.assertEqual(step.status, Status.WAITING_APPROVAL)
        self.assertEqual(archive.creator, self.user)

        self.assertEqual(archive.source, "test")
        self.assertEqual(archive.recid, "1")
        self.assertEqual(archive.source_url, source.get_record_url("1"))

    def test_harvest_not_authenticated(self):
        self.client.force_authenticate(user=None)
        self.archive1 = Archive.objects.create(
            recid="1", source="test", source_url="", creator=self.user
        )

        url = reverse("harvest", args=[self.archive1.id])
        response = self.client.post(url, format="json", follow=True)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_bagitcreate_exec(self):
        # Create an admin user and authenticate as it
        my_admin = User.objects.create_superuser("admin_test", "", "pw")
        self.client.force_authenticate(user=my_admin)

        # Create an Archive for CDS record 2798105
        url = reverse("archives-create", args=["2798105", "cds"])
        response = self.client.post(url, format="json", follow=True)

        # Check if the creation succeded and with the given data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["source"], "cds")
        self.assertEqual(response.data["recid"], "2798105")

        archive_id = response.data["id"]

        # Create an Harvest step on it
        url = reverse("harvest", args=[archive_id])
        response = self.client.post(url, format="json", follow=True)

        archive = Archive.objects.get(id=archive_id)
        steps = archive.steps.all().order_by("start_date")

        # This part will simulate what approving an Harvest step does
        # Let's execute directly what the Harvest step would do (but without celery)
        # Run bagit-create
        result = process(archive_id, steps[0].id)

        # Check that BagIt Create succeded
        self.assertEqual(result["status"], 0)
