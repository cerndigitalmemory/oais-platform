from cmath import log
from unittest import mock
from unittest.mock import patch

from django.contrib.auth.models import User
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
import ast


class UploadTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)

    def test_harvest_wrong_file_format(self):
        f1 = tempfile.NamedTemporaryFile("w+t")
        f1.seek(0)

        url = reverse("upload")

        file = TemporaryUploadedFile(
            name=f1.name, content_type="text/plain", size=0, charset="utf8"
        )
        response = self.client.post(url, {"file": file})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["detail"], "Wrong file format")
        f1.close()

    def test_harvest_wrong_source(self):
        f1 = tempfile.NamedTemporaryFile(
            "w+t", suffix=".randomfile", prefix="cds::test::"
        )
        f1.seek(0)

        url = reverse("upload")

        file = TemporaryUploadedFile(
            name=f1.name, content_type="text/plain", size=0, charset="utf8"
        )
        response = self.client.post(url, {"file": file})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["detail"], "Invalid source")
        f1.close()

    def test_upload_sip(self):
        # Prepare the mock folders and expected result from file
        with tempfile.TemporaryDirectory() as tmpdir1:
            with tempfile.TemporaryDirectory() as tmpdir2:

                # Run Bagit Create with the following parameters:
                # Save the results to tmpdir2
                res = bic.process(
                    recid="2728246",
                    source="cds",
                    target=tmpdir2,
                    loglevel=0,
                )

                print(res)

                foldername = res["foldername"]

                path_to_sip = os.path.join(os.path.abspath(tmpdir2), foldername)

                zipfile.ZipFile(f"{path_to_sip}.zip", mode="w").write(path_to_sip)

                path_to_zip = path_to_sip + ".zip"

                with open(path_to_zip, mode="rb") as myzip:

                    url = reverse("upload")

                    response = self.client.post(url, {"file": myzip})

                self.assertEqual(response.status_code, status.HTTP_200_OK)
                self.assertEqual(
                    response.data["msg"], "SIP uploading started, see Archives page"
                )
