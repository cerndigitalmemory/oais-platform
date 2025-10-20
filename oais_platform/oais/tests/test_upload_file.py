import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from celery.exceptions import Retry
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.bagit import upload
from oais_platform.settings import BIC_UPLOAD_PATH, LOCAL_UPLOAD_PATH


class UploadTest(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(
            recid="1",
            source="local",
        )
        tmp_dir = os.path.join(LOCAL_UPLOAD_PATH, "1")
        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.UPLOAD,
            status=Status.NOT_RUN,
            input_data=json.dumps({"tmp_dir": tmp_dir, "author": "name"}),
        )

    @patch("bagit_create.main.process")
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

    @patch("bagit_create.main.process")
    def test_upload_missing_input_data(self, bagit_create):
        result = upload.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Missing input data for step")
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)

    @patch("bagit_create.main.process")
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

    @patch("bagit_create.main.process")
    def test_upload_retry_failed(self, bagit_create):
        bagit_create.return_value = {"status": 1, "errormsg": "502 Bad Gateway"}

        with self.assertRaises(Retry):
            upload.apply(
                args=[self.archive.id, self.step.id, self.step.input_data], throw=True
            ).get()
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.WAITING)

    @patch("bagit_create.main.process")
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

    @patch("bagit_create.main.process")
    def test_upload_non_retriable_failed(self, bagit_create):
        bagit_create.return_value = {"status": 1, "errormsg": "Error"}

        result = upload.apply(
            args=[self.archive.id, self.step.id, self.step.input_data], throw=True
        ).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Error")
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
