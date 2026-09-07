import os
import tempfile

from django.contrib.auth.models import Permission, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.models import Archive, Status, Step, StepName


class StepViewTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="view_archive_all")
        self.execute_permission = Permission.objects.get(codename="can_execute_step")

        self.testuser = User.objects.create_user("testuser", password="pw")
        self.owner = User.objects.create_user("owner", password="pw")
        self.superuser = User.objects.create_superuser("admin", password="pw")

        self.archive = Archive.objects.create(
            recid="1",
            source="local",
            requester=self.owner,
            approver=self.superuser,
            title="",
        )

        self.harvest_step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.HARVEST,
            status=Status.COMPLETED,
        )

    def test_get_failure_types_empty(self):
        self.client.force_authenticate(user=self.superuser)

        response = self.client.get(reverse("steps-failure-types"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

    def test_get_failure_types(self):
        Step.objects.create(
            archive=self.archive,
            step_name=StepName.HARVEST,
            status=Status.FAILED,
            failure_type=StepFailureType.HTTP_403,
        )

        Step.objects.create(
            archive=self.archive,
            step_name=StepName.ARCHIVE,
            status=Status.FAILED,
            failure_type=StepFailureType.TIMEOUT,
        )

        Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            status=Status.FAILED,
            failure_type=StepFailureType.TIMEOUT,
        )

        Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            status=Status.FAILED,
            failure_type=StepFailureType.CONNECTION_ERROR,
        )

        self.client.force_authenticate(user=self.superuser)

        response = self.client.get(reverse("steps-failure-types"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            [
                StepFailureType.CONNECTION_ERROR,
                StepFailureType.HTTP_403,
                StepFailureType.TIMEOUT,
            ],
        )

    def test_download_artifact_no_artifact(self):
        self.client.force_authenticate(user=self.superuser)

        response = self.client.get(
            reverse("steps-download-artifact", args=[self.harvest_step.id])
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_download_artifact_unauthenticated(self):
        response = self.client.get(
            reverse("steps-download-artifact", args=[self.harvest_step.id])
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_download_artifact_sip(self):
        with tempfile.TemporaryDirectory() as sip_dir:
            with open(os.path.join(sip_dir, "data.txt"), "wb") as f:
                f.write(b"sip contents")

            step = Step.objects.create(
                archive=self.archive,
                step_name=StepName.HARVEST,
                status=Status.COMPLETED,
                output_data_json={
                    "artifact": {
                        "artifact_name": "SIP",
                        "artifact_localpath": sip_dir,
                    }
                },
            )

            self.client.force_authenticate(user=self.superuser)
            response = self.client.get(
                reverse("steps-download-artifact", args=[step.id])
            )

            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(response["Content-Type"], "application/zip")
            self.assertEqual(
                response["Content-Disposition"],
                f'attachment; filename="{step.id}-sip.zip"',
            )
            self.assertTrue(b"".join(response.streaming_content))

            os.remove(sip_dir + ".zip")

    def test_download_artifact_aip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            aip_path = os.path.join(tmpdir, "aip.7z")
            with open(aip_path, "wb") as f:
                f.write(b"aip contents")

            step = Step.objects.create(
                archive=self.archive,
                step_name=StepName.ARCHIVE,
                status=Status.COMPLETED,
                output_data_json={
                    "artifact": {
                        "artifact_name": "AIP",
                        "artifact_localpath": tmpdir,
                        "artifact_path": aip_path,
                    }
                },
            )

            self.client.force_authenticate(user=self.superuser)
            response = self.client.get(
                reverse("steps-download-artifact", args=[step.id])
            )

            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(response["Content-Type"], "application/x-7z-compressed")
            self.assertEqual(
                response["Content-Disposition"],
                f'attachment; filename="{step.id}-aip.7z"',
            )
            self.assertEqual(b"".join(response.streaming_content), b"aip contents")
