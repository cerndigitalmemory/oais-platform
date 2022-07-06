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

        f1.close()

    def test_upload_sip(self):
        with override_settings(BIC_UPLOAD_PATH=None):
            # Prepare a temp folder to save the results
            with tempfile.TemporaryDirectory() as tmpdir2:

                # Run Bagit Create with the following parameters:
                # Save the results to tmpdir2
                res = bic.process(
                    recid="2728246",
                    source="cds",
                    target=tmpdir2,
                    loglevel=0,
                )

                foldername = res["foldername"]

                path_to_sip = os.path.join(tmpdir2, foldername)
                path_to_zip = path_to_sip + ".zip"

                # create a ZipFile object
                with zipfile.ZipFile("test.zip", 'w') as zipf:
                    # Iterate over all the files in directory
                    len_dir_path = len(tmpdir2)
                    for root, _, files in os.walk(tmpdir2):
                        for file in files:
                            file_path = os.path.join(root, file)
                            zipf.write(file_path, file_path[len_dir_path:])
                zipf.close()
                

                with open("test.zip", mode="rb") as myzip:

                    url = reverse("upload")
                    response = self.client.post(url, {"file": myzip})
                
                os.remove("test.zip")

                self.assertEqual(response.status_code, status.HTTP_200_OK)
                
                self.assertEqual(response.data["status"], 0)
                self.assertEqual(response.data["msg"], 'SIP uploading started, see Archives page')
