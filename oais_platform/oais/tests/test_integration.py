from unittest.mock import patch

from django.contrib.auth.models import User
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import StepName, StepType


class IntegrationAPITests(APITestCase):
    """Tests the integration of the API without using "reverse".

    These tests are targeted at ensuring the flow works as expected.
    """

    def setUp(self):
        """
        Set up the test environment before each test case.
        """
        self.superuser = User.objects.create_superuser("superuser", "", "pw")
        self.test_user = User.objects.create_user("testuser", "", "pw")

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_search_and_harvest(self, mock_dispatch):
        """
        Test the search and harvest functionality by creating and unstaging a record.
        """
        self.client.force_authenticate(user=self.test_user)

        response = self.client.get(
            "/api/search/cds-rdm-sandbox/?q=Digital Memory&p=1&s=20",
            format="json",
            follow=True,
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        found = False
        record = None
        for result in response.data["results"]:
            if result["recid"] == "yz39b-yf220":
                found = True
                record = result
                break

        self.assertEqual(found, True)

        post_data = {
            "records": [
                {
                    "source_url": record["source_url"],
                    "recid": record["recid"],
                    "title": record["title"],
                    "authors": record["authors"],
                    "source": record["source"],
                }
            ]
        }

        self.client.post("/api/users/me/stage/", post_data, format="json")

        response = self.client.get(
            "/api/users/me/staging-area/", post_data, format="json"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["source"], "cds-rdm-sandbox")
        self.assertEqual(response.data["results"][0]["recid"], "yz39b-yf220")
        self.assertEqual(
            response.data["results"][0]["requester"]["id"], self.test_user.id
        )
        self.assertEqual(response.data["results"][0]["approver"], None)
        self.assertEqual(response.data["results"][0]["staged"], True)

        self.client.force_authenticate(user=self.superuser)

        response = self.client.get(
            "/api/users/me/staging-area/", post_data, format="json"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)
        result = response.data["results"][0]
        self.assertEqual(result["source"], "cds-rdm-sandbox")
        self.assertEqual(result["recid"], "yz39b-yf220")

        response = self.client.post(
            f"/api/archives/{result['id']}/unstage/", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["approver"]["id"], self.superuser.id)
        self.assertEqual(response.data["staged"], False)

        self.client.force_authenticate(user=self.test_user)

        response = self.client.get(
            "/api/users/me/staging-area/", post_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 0)

        response = self.client.get(
            f"/api/archives/{result['id']}/steps/", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)

        mock_dispatch.assert_called_once_with(
            StepType.get_by_stepname(StepName.HARVEST),
            result["id"],
            response.data[0]["id"],
            None,
            None,
            False,
        )
