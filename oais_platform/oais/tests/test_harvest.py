import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from celery.exceptions import Retry
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks.harvest import harvest
from oais_platform.settings import AGGREGATED_FILE_SIZE_LIMIT, BIC_UPLOAD_PATH


class HarvestTest(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(
            recid="1",
            source="test_source",
            original_file_size=AGGREGATED_FILE_SIZE_LIMIT - 100,
        )
        self.step = Step.objects.create(
            archive=self.archive, name=Steps.HARVEST, status=Status.WAITING
        )

    @patch("bagit_create.main.process")
    def test_harvest_success(self, bagit_create):
        sip_folder = "result_folder"
        bagit_create.return_value = {"status": 0, "foldername": sip_folder}
        fake_file1 = MagicMock()
        fake_file1.stat.return_value.st_size = AGGREGATED_FILE_SIZE_LIMIT - 100

        with patch.object(Path, "rglob", return_value=[fake_file1]):
            result = harvest.apply(
                args=[self.archive.id, self.step.id], throw=True
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

    def test_harvest_file_too_big(self):
        self.archive.set_original_file_size(AGGREGATED_FILE_SIZE_LIMIT + 100)

        result = harvest.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Record is too large to be harvested.")

    def test_harvest_too_many_concurrent_harvests(self):
        for i in range(2):
            archive = Archive.objects.create(
                recid=f"r{i}",
                source="test_source",
                original_file_size=AGGREGATED_FILE_SIZE_LIMIT / 2,
            )
            Step.objects.create(
                archive=archive, name=Steps.HARVEST, status=Status.IN_PROGRESS
            )

        self.archive.set_original_file_size(1)
        result = harvest.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(result["status"], 1)
        self.assertIn("Record is too large to be harvested", result["errormsg"])

    @patch("bagit_create.main.process")
    def test_harvest_bagit_exception(self, bagit_create):
        exc_msg = "bagit-create exception"
        bagit_create.side_effect = RuntimeError(exc_msg)

        result = harvest.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], exc_msg)
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)

    @patch("bagit_create.main.process")
    def test_harvest_retry_failed(self, bagit_create):
        bagit_create.return_value = {"status": 1, "errormsg": "502 Bad Gateway"}

        with self.assertRaises(Retry):
            harvest.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.IN_PROGRESS)

    @patch("bagit_create.main.process")
    @patch("celery.app.task.Task.request")
    def test_harvest_retries_exceeded(self, task_request, bagit_create):
        bagit_create.return_value = {"status": 1, "errormsg": "502 Bad Gateway"}

        task_request.id = "test_task_id"
        task_request.retries = 10
        result = harvest.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Max retries exceeded.")
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)

    @patch("bagit_create.main.process")
    def test_harvest_non_retriable_failed(self, bagit_create):
        bagit_create.return_value = {"status": 1, "errormsg": "Error"}

        result = harvest.apply(args=[self.archive.id, self.step.id], throw=True).get()
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], "Error")
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.FAILED)
