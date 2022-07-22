from cmath import log
import ntpath
from unittest import mock
from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import override_settings
from django.urls import reverse
from django.core.files.uploadedfile import TemporaryUploadedFile, SimpleUploadedFile
from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tests.utils import TestSource
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase
from bagit_create import main as bic

import json, tempfile, os
import zipfile
from oais_platform.settings import BIC_UPLOAD_PATH


class AnnounceTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)

    def test_announce_sip(self):
        with override_settings(BIC_UPLOAD_PATH=None):
            # Prepare a temp folder to save the results
            with tempfile.NamedTemporaryFile() as file:
                with tempfile.TemporaryDirectory() as tmpdir2:

                    # Run Bagit Create with the following parameters:
                    # Save the results to tmpdir2
                    res = bic.process(
                        recid=None,
                        source_path=os.path.join(file.name),
                        source="local",
                        target=tmpdir2,
                        loglevel=0,
                        author="test",
                    )
                    foldername = res["foldername"]
                    folder_path = os.path.join(tmpdir2, foldername)
                    url = reverse("announce")
                    response = self.client.post(url, {"announce_path": folder_path})
                    self.assertEqual(response.status_code, status.HTTP_302_FOUND)

                    self.assertRedirects(
                        response,
                        f"http://testserver/api/archives/1/details/",
                        status_code=302,
                        target_status_code=200,
                        fetch_redirect_response=True,
                    )

    def test_announce_sip_error(self):
        with override_settings(BIC_UPLOAD_PATH=None):
            # Prepare a temp folder to save the results
            with tempfile.NamedTemporaryFile() as file:
                with tempfile.TemporaryDirectory() as tmpdir2:

                    url = reverse("announce")
                    response = self.client.post(url, {"announce_path": tmpdir2})
                    self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
                    self.assertEqual(
                        response.data["detail"], "Given folder is not a valid SIP"
                    )
