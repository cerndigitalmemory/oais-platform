from unittest import mock
from unittest.mock import patch

from django.contrib.auth.models import User
from django.urls import reverse
from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tests.utils import TestSource
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase


class HarvestTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)

    def test_wrong_source(self):

        url = reverse("archives-create", args=["1", "wrong"])
        response = self.client.post(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Step.objects.count(), 0)

    @patch("oais_platform.oais.views.get_source")
    def test_harvest(self, get_source):

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
