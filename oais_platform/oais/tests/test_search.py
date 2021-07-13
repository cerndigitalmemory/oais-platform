from unittest import mock
from unittest.mock import patch

from django.contrib.auth.models import User
from django.urls import reverse
from oais_platform.oais.tests.utils import TestSource
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase


class SearchTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.token = Token.objects.create(user=self.user)
        self.client.credentials(HTTP_AUTHORIZATION="Token " + self.token.key)

    def test_search_wrong_source(self):
        url = reverse("search", args=["wrong"])
        response = self.client.get(url, {"q": "query"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["detail"], "Invalid source")

    @patch("oais_platform.oais.views.get_source")
    def test_search_missing_query(self, get_source):
        get_source.return_value = TestSource()

        url = reverse("search", args=["test"])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["detail"], "Missing parameter q")

    @patch("oais_platform.oais.views.get_source")
    def test_search(self, get_source):
        source = TestSource()
        get_source.return_value = source

        url = reverse("search", args=["test"])
        response = self.client.get(url, {"q": "query"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertNotEqual(len(get_source.call_args_list), 0)
        for args in get_source.call_args_list:
            self.assertEqual(args, mock.call("test"))

        self.assertEqual(len(response.data), 1)
        record = response.data[0]
        self.assertEqual(record["recid"], "1")
        self.assertEqual(record["url"], source.get_record_url("1"))
        self.assertEqual(record["title"], "query")
        self.assertEqual(record["source"], "test")
