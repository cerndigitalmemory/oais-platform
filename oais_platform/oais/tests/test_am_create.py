import json
from unittest.mock import MagicMock, patch

import requests
from celery.exceptions import Retry
from django_celery_beat.models import PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.archivematica import archivematica
from oais_platform.settings import AGGREGATED_FILE_SIZE_LIMIT, AM_CONCURRENCY_LIMT


class ArchivematicaCreateTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            path_to_sip="test_path",
            sip_size=1000,
        )

        self.step = Step.objects.create(
            archive=self.archive, step_name=StepName.ARCHIVE
        )

    @patch("amclient.AMClient.create_package")
    def test_archivematica_success(self, create_package):
        create_package.return_value = {"id": "test_package_id"}
        result = archivematica.apply(args=[self.archive.id, self.step.id])

        result = result.get()
        self.step.refresh_from_db()
        periodic_task = PeriodicTask.objects.latest("id")
        task_arg = json.loads(periodic_task.args)

        self.assertEqual(self.step.status, Status.WAITING)
        self.assertEqual(
            periodic_task.name, f"Archivematica status for step: {self.step.id}"
        )
        self.assertEqual(periodic_task.task, "check_am_status")
        self.assertEqual(
            task_arg,
            [
                create_package.return_value,
                self.step.id,
                self.archive.id,
                None,
            ],
        )

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_create_package(self, create_package):
        create_package.return_value = -1
        result = archivematica.apply(args=[self.archive.id, self.step.id])

        result = result.get()
        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)
        errormsg = f"AM create returned error {create_package.return_value}"

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], 1)
        self.assertIn(errormsg, step_output["errormsg"])
        self.assertEqual(result["status"], 1)
        self.assertIn(errormsg, result["errormsg"])

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_authentication(self, create_package):
        unauthorized_request = requests.Request()
        unauthorized_request.status_code = 403
        create_package.side_effect = requests.exceptions.HTTPError(
            request=unauthorized_request
        )
        result = archivematica.apply(args=[self.archive.id, self.step.id])

        result = result.get()
        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)
        errormsg = f"status code {unauthorized_request.status_code}"

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], 1)
        self.assertIn(errormsg, step_output["errormsg"])
        self.assertEqual(result["status"], 1)
        self.assertIn(errormsg, result["errormsg"])

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_other_httperror(self, create_package):
        bad_request = requests.Request()
        bad_request.status_code = 400
        create_package.side_effect = requests.exceptions.HTTPError(request=bad_request)
        result = archivematica.apply(args=[self.archive.id, self.step.id])

        result = result.get()
        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)
        errormsg = f"status code {bad_request.status_code}"

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], 1)
        self.assertIn(errormsg, step_output["errormsg"])
        self.assertEqual(result["status"], 1)
        self.assertIn(errormsg, result["errormsg"])

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_other_exception(self, create_package):
        exception_msg = "Error while archiving"
        create_package.side_effect = Exception(exception_msg)
        result = archivematica.apply(args=[self.archive.id, self.step.id])

        result = result.get()
        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], 1)
        self.assertIn(exception_msg, step_output["errormsg"])
        self.assertEqual(result["status"], 1)
        self.assertIn(exception_msg, result["errormsg"])

    @patch("oais_platform.oais.models.Step.objects.select_for_update")
    def test_archivematica_retry(self, mock_filter):
        mock_qs = MagicMock()
        mock_filtered_qs = MagicMock()
        mock_filtered_qs.count.return_value = AM_CONCURRENCY_LIMT
        mock_qs.filter.return_value = mock_filtered_qs
        mock_filter.return_value = mock_qs

        with self.assertRaises(Retry):
            archivematica.apply(args=[self.archive.id, self.step.id], throw=True)

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)
        msg = "Archivematica is busy, retrying"
        self.assertEqual(self.step.status, Status.WAITING)
        self.assertIn(msg, step_output["message"])

    @patch("oais_platform.oais.models.Step.objects.select_for_update")
    @patch("celery.app.task.Task.request")
    def test_archivematica_retries_exceeded(self, mock_task_request, mock_filter):
        mock_qs = MagicMock()
        mock_filtered_qs = MagicMock()
        mock_filtered_qs.count.return_value = AM_CONCURRENCY_LIMT
        mock_qs.filter.return_value = mock_filtered_qs
        mock_filter.return_value = mock_qs
        mock_task_request.id = "test_task_id"
        mock_task_request.retries = 10

        archivematica.apply(args=[self.archive.id, self.step.id], throw=True)

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)
        msg = "Archivematica max retries exceeded"
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertIn(msg, step_output["errormsg"])

    def test_archivematica_file_size_exceeded(self):
        self.archive.sip_size = AGGREGATED_FILE_SIZE_LIMIT + 1
        self.archive.save()
        archivematica.apply(args=[self.archive.id, self.step.id], throw=True)

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)
        msg = "SIP exceeds the Archivematica file size limit"
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertIn(msg, step_output["errormsg"])

    @patch("oais_platform.oais.models.Archive.objects.select_for_update")
    def test_archivematica_aggr_file_size_exceeded(self, mock_filter):
        mock_qs = MagicMock()
        mock_filtered_qs = MagicMock()
        mock_filtered_qs.aggregate.return_value = {"total": AGGREGATED_FILE_SIZE_LIMIT}
        mock_qs.filter.return_value = mock_filtered_qs
        mock_filter.return_value = mock_qs

        with self.assertRaises(Retry):
            archivematica.apply(args=[self.archive.id, self.step.id], throw=True)

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)
        msg = "Archivematica is busy, retrying"
        self.assertEqual(self.step.status, Status.WAITING)
        self.assertIn(msg, step_output["message"])
