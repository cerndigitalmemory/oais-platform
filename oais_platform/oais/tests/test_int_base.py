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
    def test_cds_search(self):
        # Create an admin user and authenticate as it
        my_admin = User.objects.create_superuser('admin_test', '', "pw")
        self.client.force_authenticate(user=my_admin)

        # Search for a record on CDS
        response = self.client.get("/api/search/cds/?q= Modernising the CERN CMS Trigger Rates Monitoring software&p=1&s=20", format="json", follow=True)

        found = False
        for result in response.data["results"]:
            if result["recid"] == "2798105":
                found = True

        self.assertEqual(found, True)
