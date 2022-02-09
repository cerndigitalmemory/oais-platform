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
        print(response.data)
        self.assertEqual(response.data["detail"], "Invalid source")
        f1.close()

    def test_upload_sip(self):
        # Prepare the mock folders and expected result from file
        with tempfile.TemporaryDirectory() as tmpdir1:
            with tempfile.TemporaryDirectory() as tmpdir2:
                # Creates two temp directories and two files
                f1 = tempfile.NamedTemporaryFile("w+t", dir=tmpdir1)
                f1.seek(0)

                # Run Bagit Create with the following parameters:
                # Save the results to tmpdir2
                result = bic.process(
                    recid=None,
                    source="local",
                    loglevel=0,
                    target=tmpdir2,
                    source_path=tmpdir1,
                    author="python-test",
                )

                res = ast.literal_eval(result)

                foldername = res["foldername"]

                path_to_sip = os.path.join(os.path(tmpdir2), res["foldername"])

                zipfile.ZipFile(f"{path_to_sip}.zip", mode="w").write(path_to_sip)

                file = TemporaryUploadedFile(
                    name=f"{foldername}.zip",
                    content_type="text/plain",
                    size=0,
                    charset="utf8",
                )

                url = reverse("upload")

                response = self.client.post(url, {"file": file})

                f1.close()
