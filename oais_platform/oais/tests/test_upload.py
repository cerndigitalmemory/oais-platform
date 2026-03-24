import logging
import os
import tempfile
import zipfile
from io import BytesIO
from unittest.mock import patch

from bagit_create import main as bic
from django.contrib.auth.models import User
from django.core.files.uploadedfile import TemporaryUploadedFile
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.enums import Status
from oais_platform.oais.models import Archive, Step, StepName, StepType
from oais_platform.settings import BIC_WORKDIR, SIP_UPSTREAM_BASEPATH


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

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_upload_sip(self, mock_dispatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create real SIP using bic
            res = bic.process(
                recid="yz39b-yf220",
                source="cds-rdm-sandbox",
                target=tmpdir,
                loglevel=logging.DEBUG,
                workdir=BIC_WORKDIR,
            )

            foldername = res["foldername"]
            path_to_sip = os.path.join(tmpdir, foldername)

            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, "w") as zipf:
                for root, _, files in os.walk(path_to_sip):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, path_to_sip)
                        zipf.write(file_path, arcname)

            zip_buffer.seek(0)

            url = reverse("upload-sip")
            response = self.client.post(url, {"file": zip_buffer}, format="multipart")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], 0)
        self.assertEqual(response.data["msg"], "SIP uploaded, see Archives page")

        latest_step = Step.objects.latest("id")
        latest_archive = Archive.objects.latest("id")
        expected_path = os.path.join(
            SIP_UPSTREAM_BASEPATH, "upload", f"Archive-{latest_archive.id}"
        )

        mock_dispatch.assert_called_once_with(
            StepType.get_by_stepname(StepName.VALIDATION),
            Archive.objects.latest("id").id,
            latest_step.id,
            False,
        )
        self.assertEqual(response.data["archive"], latest_archive.id)
        self.assertEqual(latest_archive.path_to_sip, expected_path)
        self.assertTrue(os.path.exists(expected_path))
        self.assertEqual(latest_step.initiated_by_user, self.user)
        self.assertEqual(latest_step.initiated_by_harvest_batch, None)

        sip_upload_step = Step.objects.get(
            archive=latest_archive, step_name=StepName.SIP_UPLOAD
        )

        self.assertEqual(sip_upload_step.status, Status.COMPLETED)
        self.assertEqual(sip_upload_step.initiated_by_user, self.user)
        self.assertEqual(sip_upload_step.initiated_by_harvest_batch, None)
        self.assertIsNotNone(sip_upload_step.finish_date)

    def test_upload_sip_corrupted_zip(self):
        fake_zip = BytesIO(b"this is not a zip file")
        fake_zip.name = "test.zip"

        url = reverse("upload-sip")
        response = self.client.post(url, {"file": fake_zip}, format="multipart")

        self.assertEqual(response.data["status"], 1)
        self.assertEqual(response.data["msg"], "SIP upload failed, see Archives page")

        step = Step.objects.latest("id")
        self.assertEqual(step.status, Status.FAILED)
        self.assertIsNotNone(step.finish_date)
        self.assertIsNotNone(step.output_data_json.get("errormsg"))

    def test_upload_sip_no_file(self):
        url = reverse("upload-sip")
        response = self.client.post(url, {"wrong_key": "test"}, format="multipart")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
