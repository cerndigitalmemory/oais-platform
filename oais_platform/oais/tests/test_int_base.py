from unittest import mock
from unittest.mock import patch

from django.contrib.auth.models import User
from django.urls import reverse
from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tests.utils import TestSource
from rest_framework import status
from rest_framework.test import APITestCase
from oais_platform.oais.tasks import process
import json


class IntegrationAPITests(APITestCase):
    """
    Those tests won't make use of "reverse". They are targeted at making sure that the flow
    executed interacting with the API works as expected (such as the ones implemented by the web interfact).
    """

    def setUp(self):
        # Create an admin user and authenticate as it
        my_admin = User.objects.create_superuser("admin_test", "", "pw")
        self.client.force_authenticate(user=my_admin)

    def test_cds_search(self):
        # Search for a record on CDS
        response = self.client.get(
            "/api/search/cds/?q= Modernising the CERN CMS Trigger Rates Monitoring software&p=1&s=20",
            format="json",
            follow=True,
        )

        found = False
        for result in response.data["results"]:
            if result["recid"] == "2798105":
                found = True

        self.assertEqual(found, True)

    def test_add_to_staging(self):
        post_data = {
            "records": [
                {
                    "source_url": "https://cds.cern.ch/record/2798105",
                    "recid": "2798105",
                    "title": "Modernising the CERN CMS Trigger Rates Monitoring software",
                    "authors": ["Vivace, Antonio"],
                    "source": "cds",
                }
            ]
        }

        # Stage the first result
        self.client.post("/api/users/me/staging-area/", post_data, format="json")

        response = self.client.get(
            "/api/users/me/staging-area/", post_data, format="json"
        )

        self.assertEqual(response.data["results"][0]["source"], "cds")
        self.assertEqual(response.data["results"][0]["recid"], "2798105")
