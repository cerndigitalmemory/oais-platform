from django.contrib.auth.models import Permission, User
from django.urls import reverse
from oais_platform.oais.models import SourceStatus
from rest_framework import status
from rest_framework.test import APITestCase


class SourceTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")

    def test_initial_source_status(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("users-me-sources")
        response = self.client.get(url, format="json")

        sources_status = response.data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(sources_status["cds"], SourceStatus.NEEDS_CONFIG_PRIVATE)
        self.assertEqual(sources_status["indico"], SourceStatus.NEEDS_CONFIG_PRIVATE)
        self.assertEqual(sources_status["codimd"], SourceStatus.NEEDS_CONFIG)
        self.assertEqual(sources_status["inveniordm"], SourceStatus.READY)
        self.assertEqual(sources_status["cod"], SourceStatus.READY)
        self.assertEqual(sources_status["zenodo"], SourceStatus.READY)

    def test_change_cds(self):
        self.client.force_authenticate(user=self.creator)

        # Changes the sso_comp token
        url = reverse("users-me")
        response = self.client.post(
            url, {"sso_comp_token": "SOME_TOKEN"}, format="json"
        )

        # Get the new statuses
        url = reverse("users-me-sources")
        response = self.client.get(url, format="json")

        sources_status = response.data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(sources_status["cds"], SourceStatus.READY)

    def test_change_indico(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("users-me")
        response = self.client.post(
            url, {"indico_api_key": "SOME_TOKEN"}, format="json"
        )

        url = reverse("users-me-sources")
        response = self.client.get(url, format="json")

        sources_status = response.data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(sources_status["indico"], SourceStatus.READY)

    def test_change_codimd(self):
        self.client.force_authenticate(user=self.creator)

        url = reverse("users-me")
        response = self.client.post(
            url, {"codimd_api_key": "SOME_TOKEN"}, format="json"
        )

        url = reverse("users-me-sources")
        response = self.client.get(url, format="json")

        sources_status = response.data
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(sources_status["codimd"], SourceStatus.READY)
