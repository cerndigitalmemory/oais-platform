import json
from unittest.mock import Mock, patch

import requests
from django.utils import timezone
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.archivematica import check_am_status
from oais_platform.settings import AM_RETRY_LIMIT, AM_URL, AM_WAITING_TIME_LIMIT


class ArchivematicaStatusTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(
            recid="1", source="test", source_url="", path_to_sip="test_path"
        )

        self.step = Step.objects.create(
            archive=self.archive, step_name=StepName.ARCHIVE
        )

        # simulate archivematica step started
        self.step.set_start_date()

    @patch("amclient.AMClient.get_jobs")
    @patch("amclient.AMClient.get_package_details")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_completed(
        self, periodic_tasks, get_unit_status, get_package_details, get_jobs
    ):
        get_jobs.return_value = [
            {
                "name": "Normalize for preservation",
                "status": "COMPLETE",
                "tasks": [
                    {
                        "exit_code": 0,
                        "uuid": 5678,
                    }
                ],
            }
        ]
        get_unit_status.return_value = {
            "status": "COMPLETE",
            "microservice": "Remove the processing directory",
            "uuid": 5678,
        }
        get_package_details.return_value = {
            "current_path": "aip_test_path",
            "uuid": 5678,
        }
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.COMPLETED)
        self.assertEqual(step_output["status"], get_unit_status.return_value["status"])
        self.assertEqual(
            step_output["microservice"], get_unit_status.return_value["microservice"]
        )
        self.assertTrue(step_output["artifact"])
        self.assertIsNone(step_output.get("retry_count", None))
        self.assertIsNone(step_output.get("retry", None))
        self.assertIsNone(step_output.get("errormsg", None))
        self.assertTrue(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_completed_not_fully(self, periodic_tasks, get_unit_status):
        get_unit_status.return_value = {
            "status": "COMPLETE",
            "microservice": "Completed first half, still processing",
            "uuid": 5678,
        }
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.IN_PROGRESS)
        self.assertEqual(step_output["status"], get_unit_status.return_value["status"])
        self.assertEqual(
            step_output["microservice"], get_unit_status.return_value["microservice"]
        )
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertIsNone(step_output.get("retry_count", None))
        self.assertIsNone(step_output.get("retry", None))
        self.assertIsNone(step_output.get("errormsg", None))
        self.assertFalse(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_package_details")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_completed_uuid_not_found(
        self, periodic_tasks, get_unit_status, get_package_details
    ):
        get_unit_status.return_value = {
            "status": "COMPLETE",
            "microservice": "Remove the processing directory",
            "uuid": 5678,
        }
        get_package_details.return_value = "Not found"
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.IN_PROGRESS)
        self.assertEqual(step_output["status"], get_unit_status.return_value["status"])
        self.assertEqual(
            step_output["microservice"], get_unit_status.return_value["microservice"]
        )
        self.assertEqual(step_output["package_retry"], 1)
        self.assertIsNone(step_output.get("retry_count", None))
        self.assertIsNone(step_output.get("retry", None))
        self.assertIsNone(step_output.get("errormsg", None))
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertFalse(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_package_details")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_completed_uuid_not_found_retry_limit(
        self, periodic_tasks, get_unit_status, get_package_details
    ):
        self.step.set_output_data({"package_retry": 5})
        get_unit_status.return_value = {
            "status": "COMPLETE",
            "microservice": "Remove the processing directory",
            "uuid": 5678,
        }
        get_package_details.return_value = "Not found"
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertIsNone(step_output.get("retry_count", None))
        self.assertIsNone(step_output.get("retry", None))
        self.assertIn("AIP package with UUID 5678 not found", step_output["errormsg"])
        self.assertTrue(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_processing(self, periodic_tasks, get_unit_status):
        get_unit_status.return_value = {
            "status": "PROCESSING",
            "microservice": "Package is being processed",
        }
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.IN_PROGRESS)
        self.assertEqual(step_output["status"], get_unit_status.return_value["status"])
        self.assertEqual(
            step_output["microservice"], get_unit_status.return_value["microservice"]
        )
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertIsNone(step_output.get("retry_count", None))
        self.assertIsNone(step_output.get("retry", None))
        self.assertIsNone(step_output.get("errormsg", None))
        self.assertFalse(periodic_tasks.delete.called)

    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_periodictask_not_found(self, periodic_tasks):
        exception_msg = "Unexpected exception occurred"
        periodic_tasks.get.side_effect = Exception(exception_msg)
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()

        self.assertEqual(self.step.status, Status.FAILED)

    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_unexpected_exception(self, periodic_tasks, get_unit_status):
        exception_msg = "Unexpected exception occurred"
        get_unit_status.side_effect = Exception(exception_msg)
        periodic_tasks.get.return_value = periodic_tasks

        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], "FAILED")
        self.assertEqual(step_output["errormsg"], exception_msg)
        self.assertTrue(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_jobs")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_bad_request_waiting(
        self, periodic_tasks, get_unit_status, get_jobs
    ):
        bad_request = requests.Response()
        bad_request.status_code = 400
        get_unit_status.side_effect = requests.exceptions.HTTPError(
            response=bad_request
        )
        periodic_tasks.get.return_value = periodic_tasks
        get_jobs.return_value = 1

        self.step.status = Status.WAITING
        self.step.save()

        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.WAITING)
        self.assertEqual(step_output["status"], "WAITING")
        self.assertEqual(
            step_output["microservice"], "Waiting for archivematica to respond"
        )
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertFalse(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_jobs")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_bad_request_waiting_limit_reached(
        self, periodic_tasks, get_unit_status, get_jobs
    ):
        bad_request = requests.Response()
        bad_request.status_code = 400
        get_unit_status.side_effect = requests.exceptions.HTTPError(
            response=bad_request
        )
        periodic_tasks.get.return_value = periodic_tasks
        get_jobs.return_value = []

        self.step.status = Status.WAITING
        self.step.start_date = timezone.now() - timezone.timedelta(
            minutes=AM_WAITING_TIME_LIMIT + 1
        )
        self.step.save()

        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], "FAILED")
        self.assertEqual(step_output["errormsg"], "Archivematica delayed to respond.")
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertTrue(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_jobs")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_bad_request_has_executed_jobs(
        self, periodic_tasks, get_unit_status, get_jobs
    ):
        bad_request = requests.Response()
        bad_request.status_code = 400
        get_unit_status.side_effect = requests.exceptions.HTTPError(
            response=bad_request
        )
        periodic_tasks.get.return_value = periodic_tasks
        get_jobs.return_value = [{"job": 1}, {"job": 2}]

        self.step.status = Status.IN_PROGRESS
        self.step.save()

        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.IN_PROGRESS)
        self.assertEqual(step_output["status"], "PROCESSING")
        self.assertEqual(
            step_output["microservice"],
            "Waiting for archivematica to continue the processing",
        )
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertFalse(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_jobs")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_bad_request_no_executed_jobs(
        self, periodic_tasks, get_unit_status, get_jobs
    ):
        bad_request = requests.Response()
        bad_request.status_code = 400
        get_unit_status.side_effect = requests.exceptions.HTTPError(
            response=bad_request
        )
        periodic_tasks.get.return_value = periodic_tasks
        get_jobs.return_value = 1

        self.step.status = Status.IN_PROGRESS
        self.step.save()

        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], "FAILED")
        self.assertEqual(step_output["errormsg"], "Archivematica delayed to respond.")
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertTrue(periodic_tasks.delete.called)

    @patch("oais_platform.oais.tasks.archivematica.create_retry_step.apply_async")
    @patch("amclient.AMClient.get_jobs")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_failed(
        self, periodic_tasks, get_unit_status, get_jobs, create_retry_step
    ):
        get_jobs.side_effect = [
            [
                {
                    "name": "Normalize for preservation",
                    "status": "FAILED",
                    "microservice": "Normalize",
                    "uuid": 5678,
                },
                {
                    "name": "Extract technical metadata",
                    "status": "FAILED",
                    "microservice": "Unzipping file",
                    "uuid": 6789,
                },
                {
                    "name": "Extract technical metadata",
                    "status": "FAILED",
                    "microservice": "Unzipping file",
                    "uuid": 6789,
                },
                {
                    "name": "Scan for viruses",
                    "status": "COMPLETE",
                    "microservice": "Moving to failed folder",
                    "uuid": 7890,
                },
            ],
            [
                {
                    "name": "SIP Creation",
                    "status": "FAILED",
                    "microservice": "Exception occured",
                    "uuid": 9876,
                }
            ],
        ]
        get_unit_status.return_value = {
            "status": "FAILED",
            "microservice": "Moving to failed folder",
            "uuid": 1111,
        }
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], get_unit_status.return_value["status"])
        self.assertEqual(
            step_output["microservice"], get_unit_status.return_value["microservice"]
        )
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertEqual(len(step_output["errormsg"]), 3)
        self.assertEqual(step_output["retry"], True)
        self.assertEqual(step_output["retry_count"], 1)
        self.assertEqual(
            step_output["errormsg"][0],
            {
                "task": "Normalize for preservation",
                "microservice": "Normalize",
                "link": f"{AM_URL}/tasks/5678",
            },
        )
        self.assertEqual(
            step_output["errormsg"][1],
            {
                "task": "Extract technical metadata",
                "microservice": "Unzipping file",
                "link": f"{AM_URL}/tasks/6789",
            },
        )
        self.assertEqual(
            step_output["errormsg"][2],
            {
                "task": "SIP Creation",
                "microservice": "Exception occured",
                "link": f"{AM_URL}/tasks/9876",
            },
        )
        self.assertTrue(periodic_tasks.delete.called)
        create_retry_step.assert_called_once()

    @patch("amclient.AMClient.get_jobs")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_bad_request_error_executed_jobs(
        self, periodic_tasks, get_unit_status, get_jobs
    ):
        bad_request = requests.Response()
        bad_request.status_code = 400
        get_unit_status.side_effect = requests.exceptions.HTTPError(
            response=bad_request
        )
        periodic_tasks.get.return_value = periodic_tasks
        get_jobs.side_effect = requests.exceptions.HTTPError(response=bad_request)

        self.step.status = Status.IN_PROGRESS
        self.step.save()

        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], "FAILED")
        self.assertEqual(step_output["errormsg"], "Archivematica delayed to respond.")
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertTrue(periodic_tasks.delete.called)

    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_bad_request_unauthorized(self, periodic_tasks, get_unit_status):
        bad_request = requests.Response()
        bad_request.status_code = 403
        get_unit_status.side_effect = requests.exceptions.HTTPError(
            response=bad_request
        )
        periodic_tasks.get.return_value = periodic_tasks

        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], "FAILED")
        self.assertEqual(
            step_output["errormsg"], "Error: Could not connect to archivematica"
        )
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertTrue(periodic_tasks.delete.called)

    @patch("oais_platform.oais.tasks.archivematica.create_retry_step.apply_async")
    @patch("oais_platform.oais.tasks.archivematica.requests.get")
    @patch("amclient.AMClient.get_jobs")
    @patch("amclient.AMClient.get_package_details")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_completed_with_warnings(
        self,
        periodic_tasks,
        get_unit_status,
        get_package_details,
        get_jobs,
        get_task,
        mock_create_retry_step,
    ):
        self.step.input_data = json.dumps({"retry_count": 1})
        self.step.input_step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.ARCHIVE,
            status=Status.COMPLETED,
        )
        self.step.save()

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": "5678", "file_name": "failed.txt"}
        get_task.return_value = mock_response
        get_jobs.return_value = [
            {
                "name": "Normalize for preservation",
                "status": "COMPLETE",
                "tasks": [
                    {
                        "exit_code": 1,
                        "uuid": 5678,
                    },
                    {
                        "exit_code": 2,
                        "uuid": 1234,
                    },
                ],
            }
        ]
        get_unit_status.return_value = {
            "status": "COMPLETE",
            "microservice": "Remove the processing directory",
            "uuid": 6789,
        }
        get_package_details.return_value = {
            "current_path": "aip_test_path",
            "uuid": 7890,
        }
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 6789}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.COMPLETED_WITH_WARNINGS)
        self.assertEqual(step_output["status"], get_unit_status.return_value["status"])
        self.assertEqual(
            step_output["microservice"], get_unit_status.return_value["microservice"]
        )
        self.assertTrue(step_output["artifact"])
        self.assertEqual(step_output["retry_count"], 2)
        self.assertEqual(step_output["retry"], True)
        self.assertEqual(len(step_output["errormsg"]), 1)
        self.assertEqual(
            step_output["errormsg"][0],
            {
                "task": "Normalize for preservation",
                "filename": "failed.txt",
                "link": f"{AM_URL}/task/5678",
            },
        )
        self.assertTrue(periodic_tasks.delete.called)
        mock_create_retry_step.assert_called_once()

    @patch("oais_platform.oais.tasks.archivematica.create_retry_step.apply_async")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_user_input(
        self, periodic_tasks, get_unit_status, create_retry_step
    ):
        get_unit_status.return_value = {
            "status": "USER_INPUT",
            "microservice": "Scan for viruses",
        }
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], get_unit_status.return_value["status"])
        self.assertEqual(
            step_output["microservice"], get_unit_status.return_value["microservice"]
        )
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertEqual(step_output["retry"], True)
        self.assertEqual(step_output["retry_count"], 1)
        self.assertTrue(periodic_tasks.delete.called)
        create_retry_step.assert_called_once()

    @patch("oais_platform.oais.tasks.archivematica.create_retry_step.apply_async")
    @patch("amclient.AMClient.get_unit_status")
    @patch("django_celery_beat.models.PeriodicTask.objects")
    def test_am_status_retry_exceeded(
        self, periodic_tasks, get_unit_status, create_retry_step
    ):
        self.step.input_data = json.dumps({"retry_count": AM_RETRY_LIMIT})
        self.step.input_step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.ARCHIVE,
            status=Status.COMPLETED,
        )
        self.step.save()

        get_unit_status.return_value = {
            "status": "USER_INPUT",
            "microservice": "Scan for viruses",
        }
        periodic_tasks.get.return_value = periodic_tasks
        check_am_status.apply(args=[{"id": 1234}, self.step.id, self.archive.id, None])

        self.step.refresh_from_db()
        step_output = json.loads(self.step.output_data)

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(step_output["status"], get_unit_status.return_value["status"])
        self.assertEqual(
            step_output["microservice"], get_unit_status.return_value["microservice"]
        )
        self.assertRaises(KeyError, lambda: step_output["artifact"])
        self.assertEqual(step_output["retry"], False)
        self.assertEqual(step_output["retry_limit_exceeded"], True)
        self.assertTrue(periodic_tasks.delete.called)
        create_retry_step.assert_not_called()
