from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.settings import FILE_UPLOAD_MAX_SIZE_BYTE


class GetConfigTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)
        self.url = reverse("app_config")

    def test_get_config(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["maxFileSize"], FILE_UPLOAD_MAX_SIZE_BYTE)
