import json
import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.contrib.auth.models import Permission, User
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
        self.tmp_dir = os.path.join(LOCAL_UPLOAD_PATH, "1")
        self.author_name = "name"
        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.FILE_UPLOAD,
            status=Status.NOT_RUN,
            input_data=json.dumps(
                {"tmp_dir": self.tmp_dir, "author": self.author_name}
            ),
        )
        os.makedirs(self.tmp_dir, exist_ok=True)

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
        self.assertFalse(os.path.exists(self.tmp_dir))
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.COMPLETED)

    def test_upload_missing_input_data(self, bagit_create):
        result = upload.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Missing input data for step")
        self.assertTrue(os.path.exists(self.tmp_dir))
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
        self.assertEqual(result["tmp_dir"], self.tmp_dir)
        self.assertEqual(result["author"], self.author_name)
        self.assertTrue(os.path.exists(self.tmp_dir))
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)

    def test_upload_bagit_unsuccessful_status(self, bagit_create):
        error_msg = "An error occurred"
        bagit_create.return_value = {"status": 1, "errormsg": error_msg}

        result = upload.apply(
            args=[self.archive.id, self.step.id, self.step.input_data], throw=True
        ).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], error_msg)
        self.assertEqual(result["tmp_dir"], self.tmp_dir)
        self.assertEqual(result["author"], self.author_name)
        self.assertTrue(os.path.exists(self.tmp_dir))
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)


@patch(
    "hashlib.md5",
    side_effect=lambda x, **kwargs: MagicMock(hexdigest=lambda: "mock_recid"),
)
@patch("oais_platform.oais.views.run_step")
class UploadFileEndpointTest(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="can_upload_file")
        self.user = User.objects.create_user("user", "", "pw")
        self.user.user_permissions.add(self.permission)
        self.user.save()
        self.client.force_authenticate(user=self.user)
        self.url = reverse("upload-file")

        self.file_content = b"This is a test file content."
        self.uploaded_file_name = "test_document.txt"
        self.uploaded_file = SimpleUploadedFile(
            self.uploaded_file_name, self.file_content, content_type="text/plain"
        )

        self.expected_tmp_dir = os.path.join(LOCAL_UPLOAD_PATH, "mock_recid")

    def tearDown(self):
        if os.path.exists(self.expected_tmp_dir):
            shutil.rmtree(self.expected_tmp_dir)

    def test_upload_success(self, mock_run_step, mock_recid):
        title = "Test title"
        author = "Test author"
        data = {"file": self.uploaded_file, "title": title, "author": author}
        response = self.client.post(self.url, data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], 0)

        archive = Archive.objects.get()
        step = Step.objects.get()

        self.assertEqual(archive.recid, "mock_recid")
        self.assertEqual(archive.requester, self.user)
        self.assertEqual(archive.source, "local")
        self.assertEqual(archive.title, title)

        self.assertEqual(step.archive, archive)
        self.assertEqual(step.step_type.name, StepName.FILE_UPLOAD)
        self.assertEqual(step.status, Status.NOT_RUN)
        self.assertEqual(
            json.loads(step.input_data),
            {
                "tmp_dir": self.expected_tmp_dir,
                "author": author,
            },
        )
        self.assertEqual(step.initiated_by_user, self.user)
        self.assertEqual(step.initiated_by_harvest_batch, None)

        mock_run_step.assert_called_once_with(step, archive.id)

    def test_upload_no_title_no_author(self, mock_run_step, mock_recid):
        data = {"file": self.uploaded_file}
        response = self.client.post(self.url, data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["status"], 0)

        archive = Archive.objects.get()
        step = Step.objects.get()

        self.assertEqual(archive.recid, "mock_recid")
        self.assertEqual(archive.requester, self.user)
        self.assertEqual(archive.source, "local")
        self.assertEqual(archive.title, "local - mock_recid")

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
        self.assertEqual(step.initiated_by_user, self.user)
        self.assertEqual(step.initiated_by_harvest_batch, None)

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

    @patch("shutil.move")
    def test_upload_failed_processing(self, mock_move, mock_run_step, mock_recid):
        error_message = "Failed to move file"
        mock_move.side_effect = RuntimeError(error_message)

        data = {"file": self.uploaded_file}
        response = self.client.post(self.url, data)

        self.assertEqual(response.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)

        archive = Archive.objects.get()
        step = Step.objects.get()

        self.assertEqual(archive.recid, "mock_recid")
        self.assertEqual(archive.requester, self.user)
        self.assertEqual(archive.source, "local")

        self.assertEqual(step.archive, archive)
        self.assertEqual(step.step_type.name, StepName.FILE_UPLOAD)
        self.assertEqual(step.status, Status.FAILED)
        self.assertEqual(
            json.loads(step.output_data),
            {
                "status": 1,
                "errormsg": f"Error occurred while processing file: {error_message}",
                "archive": archive.id,
            },
        )
        self.assertEqual(step.initiated_by_user, self.user)
        self.assertEqual(step.initiated_by_harvest_batch, None)

        mock_run_step.assert_not_called()
