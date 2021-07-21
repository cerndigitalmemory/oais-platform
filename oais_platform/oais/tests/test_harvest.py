from unittest import mock
from unittest.mock import patch

from django.contrib.auth.models import User
from django.urls import reverse
from oais_platform.oais.models import Archive, ArchiveStatus, Record
from oais_platform.oais.tests.utils import TestSource
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase


class HarvestTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)


    def test_harvest_wrong_source(self):
        url = reverse("harvest", args=["1", "wrong"])
        response = self.client.post(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["detail"], "Invalid source")
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Record.objects.count(), 0)

    @patch("oais_platform.oais.views.get_source")
    def test_harvest(self, get_source):
        source = TestSource()
        get_source.return_value = source

        url = reverse("harvest", args=["1", "test"])
        response = self.client.post(url, format="json", follow=True)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertNotEqual(len(get_source.call_args_list), 0)
        for args in get_source.call_args_list:
            self.assertEqual(args, mock.call("test"))

        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Record.objects.count(), 1)

        archive = Archive.objects.all()[0]
        self.assertEqual(archive.status, ArchiveStatus.WAITING_APPROVAL)
        self.assertEqual(archive.creator, self.user)

        record = archive.record
        self.assertEqual(record.source, "test")
        self.assertEqual(record.recid, "1")
        self.assertEqual(record.url, source.get_record_url("1"))

    def test_harvest_not_authenticated(self):
        self.client.force_authenticate(user=None)

        url = reverse("harvest", args=["1", "test"])
        response = self.client.post(url, format="json", follow=True)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
