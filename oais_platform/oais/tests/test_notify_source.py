from unittest.mock import Mock, patch

import requests
from django.contrib.auth.models import User
from rest_framework.test import APITestCase

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.models import (
    ApiKey,
    Archive,
    ArchiveState,
    Profile,
    Source,
    Status,
    Step,
    StepName,
)
from oais_platform.oais.tasks.notify_source import notify_source


class NotifySourceTests(APITestCase):

    def setUp(self):
        self.testuser = User.objects.create_user("testuser", password="pw")
        self.source = Source.objects.create(
            name="test",
            longname="Test",
            api_url="test.test/api",
            classname="Local",
            notification_endpoint="test.test/api/notify",
            notification_enabled=True,
        )
        self.system_user = Profile.objects.get(system=True).user

        self.system_user_api_key = ApiKey.objects.create(
            user=self.system_user, source=self.source, key="system1234"
        )

        self.archive = Archive.objects.create(
            recid="1",
            source=self.source.name,
            source_url="",
            requester=self.testuser,
            title="",
            state=ArchiveState.SIP,
            path_to_sip="sip/test/path",
        )

        Step.objects.create(archive=self.archive, step_name=StepName.HARVEST)

        self.step = Step.objects.create(
            archive=self.archive, step_name=StepName.NOTIFY_SOURCE
        )

    def setup_aip(self):
        Step.objects.create(
            archive=self.archive, step_name=StepName.HARVEST, status=Status.COMPLETED
        )
        Step.objects.create(
            archive=self.archive, step_name=StepName.ARCHIVE, status=Status.COMPLETED
        )

        self.archive.set_aip_path("aip/test/path2")

    def test_notify_source_not_aip(self):
        result = notify_source(self.archive.id, self.step.id)

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"], f"Archive {self.archive.id} is not an AIP."
        )
        self.step.refresh_from_db()
        self.assertEqual(self.step.failure_type, StepFailureType.PATH_NOT_FOUND)

    def test_notify_source_no_source_obj(self):
        self.setup_aip()
        self.archive.source = "new source"
        self.archive.save()

        result = notify_source(self.archive.id, self.step.id)

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"],
            f"Source object with name {self.archive.source} does not exist.",
        )

    def test_notify_source_notification_disabled(self):
        self.setup_aip()
        self.source.notification_enabled = False
        self.source.save()

        result = notify_source(self.archive.id, self.step.id)

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"],
            f"Notify source disabled for {self.archive.source}.",
        )

    def test_notify_source_no_notification_endpoint(self):
        self.setup_aip()
        self.source.notification_endpoint = None
        self.source.save()

        result = notify_source(self.archive.id, self.step.id)

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"],
            f"Archive's source ({self.archive.source}) has no notification endpoint set.",
        )

    def test_notify_source_no_implementation(self):
        self.setup_aip()
        self.source.classname = "LocalNotifyNotImpl"
        self.source.save()

        result = notify_source(self.archive.id, self.step.id)

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"], "Step Notify Source not implemented for this Source."
        )

    def test_notify_source_success(self):
        self.setup_aip()

        result = notify_source(self.archive.id, self.step.id)

        self.assertEqual(result["status"], 0)
        self.assertEqual(result["errormsg"], None)

    @patch("oais_platform.oais.tasks.notify_source.get_source")
    def test_notify_source_raises_http_error(self, mock_get_source):
        self.setup_aip()
        response = requests.Response()
        response.status_code = 502
        mock_source = Mock()
        mock_source.notify_source.side_effect = requests.exceptions.HTTPError(
            response=response
        )
        mock_get_source.return_value = mock_source

        result = notify_source(self.archive.id, self.step.id)

        self.assertEqual(result["status"], 1)
        self.step.refresh_from_db()
        self.assertEqual(self.step.failure_type, StepFailureType.HTTP_502)

    @patch("oais_platform.oais.tasks.notify_source.get_source")
    def test_notify_source_raises_connection_error(self, mock_get_source):
        self.setup_aip()
        mock_source = Mock()
        mock_source.notify_source.side_effect = ConnectionResetError()
        mock_get_source.return_value = mock_source

        result = notify_source(self.archive.id, self.step.id)

        self.assertEqual(result["status"], 1)
        self.step.refresh_from_db()
        self.assertEqual(self.step.failure_type, StepFailureType.CONNECTION_ERROR)
