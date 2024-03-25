import json
from unittest.mock import patch

import requests
from django.test import override_settings
from django_celery_beat.models import PeriodicTask
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tasks import archivematica


@override_settings(CELERY_TASK_ALWAYS_EAGER=True, CELERY_TASK_EAGER_PROPOGATES=True)
@override_settings(AM_REL_DIRECTORY="test_rel_dir")
class ArchivematicaCreateTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(
            recid="1", source="test", source_url="", path_to_sip="test_path"
        )

        self.step = Step.objects.create(archive=self.archive, id=1, name=5)

    @patch("amclient.AMClient.create_package")
    def test_archivematica_success(self, create_package):
        create_package.return_value = 0
        result = archivematica.delay(1, 1, None)

        result = result.get()
        step = Step.objects.get(pk=1)
        periodic_task = PeriodicTask.objects.latest("id")
        task_arg = json.loads(periodic_task.args)

        self.assertEqual(step.status, Status.WAITING)
        self.assertEqual(
            periodic_task.name, f"Archivematica status for step: {self.step.id}"
        )
        self.assertEqual(periodic_task.task, "check_am_status")
        self.assertEqual(
            task_arg,
            [
                create_package.return_value,
                step.id,
                self.archive.id,
                self.archive.path_to_sip + "::Archive_" + str(self.archive.id),
            ],
        )

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_create_package(self, create_package):
        create_package.return_value = -1
        result = archivematica.delay(1, 1, None)

        result = result.get()
        step = Step.objects.get(pk=1)
        step_output = json.loads(step.output_data)
        errormsg = f"AM Create package returned {create_package.return_value}. This may be a configuration error. Check AM logs for more information."

        self.assertEqual(step.status, Status.FAILED)
        self.assertEqual(step_output["status"], 1)
        self.assertEqual(step_output["errormsg"], errormsg)
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], errormsg)

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_authentication(self, create_package):
        unauthorized_request = requests.Request()
        unauthorized_request.status_code = 403
        create_package.side_effect = requests.exceptions.HTTPError(
            request=unauthorized_request
        )
        result = archivematica.delay(1, 1, None)

        result = result.get()
        step = Step.objects.get(pk=1)
        step_output = json.loads(step.output_data)
        errormsg = "Check your archivematica credentials (403)."

        self.assertEqual(step.status, Status.FAILED)
        self.assertEqual(step_output["status"], 1)
        self.assertEqual(step_output["errormsg"], errormsg)
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], errormsg)

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_other_httperror(self, create_package):
        unauthorized_request = requests.Request()
        unauthorized_request.status_code = 400
        create_package.side_effect = requests.exceptions.HTTPError(
            request=unauthorized_request
        )
        result = archivematica.delay(1, 1, None)

        result = result.get()
        step = Step.objects.get(pk=1)
        step_output = json.loads(step.output_data)
        errormsg = f"Check your archivematica settings configuration. ({unauthorized_request.status_code})"

        self.assertEqual(step.status, Status.FAILED)
        self.assertEqual(step_output["status"], 1)
        self.assertEqual(step_output["errormsg"], errormsg)
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], errormsg)

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_other_exception(self, create_package):
        exception_msg = "Unexpected exception occurred"
        create_package.side_effect = Exception(exception_msg)
        result = archivematica.delay(1, 1, None)

        result = result.get()
        step = Step.objects.get(pk=1)
        step_output = json.loads(step.output_data)

        self.assertEqual(step.status, Status.FAILED)
        self.assertEqual(step_output["status"], 1)
        self.assertEqual(step_output["errormsg"], exception_msg)
        self.assertEqual(result["status"], 1)
        self.assertEqual(result["errormsg"], exception_msg)
