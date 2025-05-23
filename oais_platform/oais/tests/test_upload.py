import os
import tempfile
import zipfile
from unittest.mock import patch

from bagit_create import main as bic
from django.contrib.auth.models import User
from django.core.files.uploadedfile import TemporaryUploadedFile
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Step


class UploadTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("user", "", "pw")
        self.client.force_authenticate(user=self.user)

    def test_harvest_forbidden(self):
        testuser = User.objects.create_user("testuser", "", "pw")
        self.client.force_authenticate(user=testuser)

        f1 = tempfile.NamedTemporaryFile("w+t")
        f1.seek(0)

        url = reverse("upload-sip")

        file = TemporaryUploadedFile(
            name=f1.name, content_type="text/plain", size=0, charset="utf8"
        )
        response = self.client.post(url, {"file": file})
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        f1.close()

    def test_harvest_wrong_file_format(self):
        f1 = tempfile.NamedTemporaryFile("w+t")
        f1.seek(0)

        url = reverse("upload-sip")

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

        url = reverse("upload-sip")

        file = TemporaryUploadedFile(
            name=f1.name, content_type="text/plain", size=0, charset="utf8"
        )
        response = self.client.post(url, {"file": file})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        f1.close()

    @patch("oais_platform.oais.tasks.validate.delay")
    def test_upload_sip(self, validate_delay):
        with override_settings(BIC_UPLOAD_PATH=None):
            # Prepare a temp folder to save the results
            with tempfile.TemporaryDirectory() as tmpdir2:
                # Run Bagit Create with the following parameters:
                # Save the results to tmpdir2
                res = bic.process(
                    recid="yz39b-yf220",
                    source="cds-rdm-sandbox",
                    target=tmpdir2,
                    loglevel=0,
                )

                foldername = res["foldername"]

                path_to_sip = os.path.join(tmpdir2, foldername)
                path_to_zip = path_to_sip + ".zip"

                # create a ZipFile object
                with zipfile.ZipFile("test.zip", "w") as zipf:
                    # Iterate over all the files in directory
                    len_dir_path = len(tmpdir2)
                    for root, _, files in os.walk(tmpdir2):
                        for file in files:
                            file_path = os.path.join(root, file)
                            zipf.write(file_path, file_path[len_dir_path:])
                zipf.close()

                with open("test.zip", mode="rb") as myzip:
                    url = reverse("upload-sip")
                    response = self.client.post(url, {"file": myzip})

                os.remove("test.zip")

                self.assertEqual(response.status_code, status.HTTP_200_OK)

                self.assertEqual(response.data["status"], 0)
                self.assertEqual(
                    response.data["msg"], "SIP uploaded, see Archives page"
                )
                latest_step = Step.objects.latest("id")
                validate_delay.assert_called_once_with(
                    Archive.objects.latest("id").id,
                    latest_step.id,
                    latest_step.output_data,
                    None,
                )
