import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from celery.exceptions import Retry
from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.create_sip import upload
from oais_platform.settings import BIC_UPLOAD_PATH, LOCAL_UPLOAD_PATH


@patch("bagit_create.main.process")
class UploadTaskTest(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(
            recid="1",
            source="local",
        )
        tmp_dir = os.path.join(LOCAL_UPLOAD_PATH, "1")
        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.FILE_UPLOAD,
            status=Status.NOT_RUN,
            input_data=json.dumps({"tmp_dir": tmp_dir, "author": "name"}),
        )

    def test_upload_success(self, bagit_create):
        sip_folder = "result_folder"
        bagit_create.return_value = {"status": 0, "foldername": sip_folder}
        mock_file = MagicMock()
        mock_file.stat.return_value.st_size = 100

        with patch.object(Path, "rglob", return_value=[mock_file]):
            result = upload.apply(
                args=[self.archive.id, self.step.id, self.step.input_data], throw=True
            ).get()
        self.assertEqual(result["status"], 0)
        self.assertEqual(result["foldername"], sip_folder)
        self.assertEqual(result["artifact"]["artifact_name"], "SIP")
        self.assertEqual(
            result["artifact"]["artifact_localpath"],
            os.path.join(BIC_UPLOAD_PATH, sip_folder),
        )
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.COMPLETED)

    def test_upload_missing_input_data(self, bagit_create):
        result = upload.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Missing input data for step")
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)

    def test_upload_bagit_exception(self, bagit_create):
        exc_msg = "bagit-create exception"
        bagit_create.side_effect = RuntimeError(exc_msg)

        result = upload.apply(
            args=[self.archive.id, self.step.id, self.step.input_data], throw=True
        ).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], exc_msg)
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)

    def test_upload_retry_failed(self, bagit_create):
        bagit_create.return_value = {"status": 1, "errormsg": "502 Bad Gateway"}

        with self.assertRaises(Retry):
            upload.apply(
                args=[self.archive.id, self.step.id, self.step.input_data], throw=True
            ).get()
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.WAITING)

    @patch("celery.app.task.Task.request")
    def test_upload_retries_exceeded(self, task_request, bagit_create):
        bagit_create.return_value = {"status": 1, "errormsg": "502 Bad Gateway"}

        task_request.id = "test_task_id"
        task_request.retries = 10
        result = upload.apply(
            args=[self.archive.id, self.step.id, self.step.input_data], throw=True
        ).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Max retries exceeded.")
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)

    def test_upload_non_retriable_failed(self, bagit_create):
        bagit_create.return_value = {"status": 1, "errormsg": "Error"}

        result = upload.apply(
            args=[self.archive.id, self.step.id, self.step.input_data], throw=True
        ).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Error")
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)


@patch(
    "hashlib.md5",
    side_effect=lambda x, **kwargs: MagicMock(hexdigest=lambda: "mock_recid"),
)
@patch("oais_platform.oais.views.run_step")
class UploadFileEndpointTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("user", "", "pw")
        self.client.force_authenticate(user=self.user)
        self.url = reverse("upload-file")

        self.file_content = b"This is a test file content."
        self.uploaded_file_name = "test_document.txt"
        self.uploaded_file = SimpleUploadedFile(
            self.uploaded_file_name, self.file_content, content_type="text/plain"
        )

        self.expected_tmp_dir = os.path.join(LOCAL_UPLOAD_PATH, "mock_recid")

    def test_upload_success(self, mock_run_step, mock_recid):
        data = {"file": self.uploaded_file}
        response = self.client.post(self.url, data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], 0)

        archive = Archive.objects.get()
        step = Step.objects.get()

        self.assertEqual(archive.recid, "mock_recid")
        self.assertEqual(archive.requester, self.user)
        self.assertEqual(archive.source, "local")

        self.assertEqual(step.archive, archive)
        self.assertEqual(step.step_type.name, StepName.FILE_UPLOAD)
        self.assertEqual(step.status, Status.NOT_RUN)
        self.assertEqual(
            json.loads(step.input_data),
            {
                "tmp_dir": self.expected_tmp_dir,
                "author": self.user.username,
            },
        )

        mock_run_step.assert_called_once_with(step, archive.id)

    def test_upload_missing_file(self, mock_run_step, mock_recid):
        response = self.client.post(self.url, {})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.data["detail"], "File missing")
        mock_run_step.assert_not_called()

    def test_upload_forbidden(self, mock_run_step, mock_recid):
        testuser = User.objects.create_user("testuser", "", "pw")
        self.client.force_authenticate(user=testuser)
        data = {"file": self.uploaded_file}
        response = self.client.post(self.url, data)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        mock_run_step.assert_not_called()
