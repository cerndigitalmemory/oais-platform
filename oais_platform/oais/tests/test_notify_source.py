from django.contrib.auth.models import User
from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    ArchiveState,
    Source,
    Status,
    Step,
    Steps,
)
from oais_platform.oais.tasks import notify_source


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
        self.testuser_api_key = ApiKey.objects.create(
            user=self.testuser, source=self.source, key="abcd1234"
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

        Step.objects.create(archive=self.archive, name=Steps.HARVEST)

        self.step = Step.objects.create(archive=self.archive, name=Steps.NOTIFY_SOURCE)

    def setup_aip(self):
        Step.objects.create(
            archive=self.archive, name=Steps.ARCHIVE, status=Status.COMPLETED
        )

        self.archive.set_aip_path("aip/test/path2")

    def test_notify_source_not_aip(self):
        result = notify_source(
            self.archive.id, self.step.id, api_key=self.testuser_api_key.key
        )

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"], f"Archive {self.archive.id} is not an AIP."
        )

    def test_notify_source_no_source_obj(self):
        self.setup_aip()
        self.archive.source = "new source"
        self.archive.save()

        result = notify_source(
            self.archive.id, self.step.id, api_key=self.testuser_api_key.key
        )

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"],
            f"Source object with name {self.archive.source} does not exist.",
        )

    def test_notify_source_notification_disabled(self):
        self.setup_aip()
        self.source.notification_enabled = False
        self.source.save()

        result = notify_source(
            self.archive.id, self.step.id, api_key=self.testuser_api_key.key
        )

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"],
            f"Notify source disabled for {self.archive.source}.",
        )

    def test_notify_source_no_notification_endpoint(self):
        self.setup_aip()
        self.source.notification_endpoint = None
        self.source.save()

        result = notify_source(
            self.archive.id, self.step.id, api_key=self.testuser_api_key.key
        )

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"],
            f"Archive's source ({self.archive.source}) has no notification endpoint set.",
        )

    def test_notify_source_no_implementation(self):
        self.setup_aip()
        self.source.classname = "LocalNotifyNotImpl"
        self.source.save()

        result = notify_source(
            self.archive.id, self.step.id, api_key=self.testuser_api_key.key
        )

        self.assertEqual(result["status"], 1)
        self.assertEqual(
            result["errormsg"], "Step Notify Source not implemented for this Source."
        )

    def test_notify_source_success(self):
        self.setup_aip()

        result = notify_source(
            self.archive.id, self.step.id, api_key=self.testuser_api_key.key
        )

        self.assertEqual(result["status"], 0)
        self.assertEqual(result["errormsg"], None)
