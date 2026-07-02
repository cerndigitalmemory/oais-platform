import os
import tempfile
from unittest.mock import patch

import requests
from rest_framework.test import APITestCase

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.archivematica import archivematica
from oais_platform.oais.tasks.utils import generate_directory_structure
from oais_platform.settings import AM_INSTANCES


class ArchivematicaCreateTests(APITestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.sip_base_path = os.path.join(self.tmpdir.name, "sips")
        self.path_to_sip = os.path.join(self.sip_base_path, "test_path")
        os.makedirs(self.path_to_sip)

        self.archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            title="Test archive",
            path_to_sip=self.path_to_sip,
            sip_size=1000,
        )

        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.ARCHIVE,
            input_data_json={"archivematica_instance": AM_INSTANCES[0]["AM_INSTANCE"]},
        )
        self.step.step_type.size_limit_bytes = 2000
        self.step.step_type.concurrency_limit = 5
        self.step.step_type.save()
        self.instance_patch = patch(
            "oais_platform.oais.tasks.archivematica.ArchivematicaInstances.get_instance_config",
            return_value={
                **AM_INSTANCES[0],
                "SIP_UPSTREAM_BASEPATH": self.sip_base_path,
                "AM_TRANSFER_SOURCE": "test-transfer-source",
            },
        )
        self.instance_patch.start()

    def tearDown(self):
        self.instance_patch.stop()
        self.tmpdir.cleanup()

    @patch("amclient.AMClient.create_package")
    def test_archivematica_success(self, create_package):
        create_package.return_value = {"id": "test_package_id"}
        result = archivematica.apply(args=[self.step.id])

        result = result.get()
        self.step.refresh_from_db()

        self.assertEqual(self.step.status, Status.SUBMITTED)
        self.assertEqual(self.step.output_data_json["status"], 0)
        self.assertEqual(self.step.output_data_json["package_uuid"], "test_package_id")
        self.assertEqual(self.step.step_type.current_count, 1)
        self.assertEqual(self.step.step_type.current_size_bytes, self.archive.sip_size)

    def test_archivematica_uses_path_relative_to_transfer_source_root(self):
        class FakeAMClient:
            def create_package(self):
                return {"id": "test_package_id"}

        fake_am = FakeAMClient()

        with patch(
            "oais_platform.oais.tasks.archivematica.get_am_client",
            return_value=fake_am,
        ):
            result = archivematica.apply(args=[self.step.id])

        result.get()
        transfer_source_path = generate_directory_structure(
            self.sip_base_path, self.archive
        )
        expected_transfer_directory = os.path.join(
            "/",
            os.path.relpath(
                os.path.join(transfer_source_path, os.path.basename(self.path_to_sip)),
                self.sip_base_path,
            ),
        )

        self.assertEqual(fake_am.transfer_directory, expected_transfer_directory)

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_create_package(self, create_package):
        create_package.return_value = -1
        result = archivematica.apply(args=[self.step.id])

        result = result.get()
        self.step.refresh_from_db()
        message = f"AM create returned error {create_package.return_value}"
        errormsg = "Unknown return from amclient, check logs"

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(self.step.output_data_json["status"], 1)
        self.assertIn(errormsg, self.step.output_data_json["errormsg"])
        self.assertEqual(result["status"], 1)
        self.assertIn(errormsg, result["errormsg"])
        self.assertIn(message, self.step.output_data_json["message"])
        self.assertIn(message, result["message"])
        self.assertEqual(self.step.step_type.current_count, 0)
        self.assertEqual(self.step.step_type.current_size_bytes, 0)

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_authentication(self, create_package):
        unauthorized_request = requests.Request()
        unauthorized_request.status_code = 403
        create_package.side_effect = requests.exceptions.HTTPError(
            request=unauthorized_request
        )
        result = archivematica.apply(args=[self.step.id])

        result = result.get()
        self.step.refresh_from_db()
        errormsg = f"status code {unauthorized_request.status_code}"

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(self.step.output_data_json["status"], 1)
        self.assertIn(errormsg, self.step.output_data_json["errormsg"])
        self.assertEqual(result["status"], 1)
        self.assertIn(errormsg, result["errormsg"])
        self.assertEqual(self.step.step_type.current_count, 0)
        self.assertEqual(self.step.step_type.current_size_bytes, 0)
        self.assertEqual(self.step.failure_type, StepFailureType.HTTP_403)

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_other_httperror(self, create_package):
        bad_request = requests.Request()
        bad_request.status_code = 400
        create_package.side_effect = requests.exceptions.HTTPError(request=bad_request)
        result = archivematica.apply(args=[self.step.id])

        result = result.get()
        self.step.refresh_from_db()
        errormsg = f"status code {bad_request.status_code}"

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(self.step.output_data_json["status"], 1)
        self.assertIn(errormsg, self.step.output_data_json["errormsg"])
        self.assertEqual(result["status"], 1)
        self.assertIn(errormsg, result["errormsg"])
        self.assertEqual(self.step.step_type.current_count, 0)
        self.assertEqual(self.step.step_type.current_size_bytes, 0)
        self.assertEqual(self.step.failure_type, StepFailureType.HTTP_400)

    @patch("amclient.AMClient.create_package")
    def test_archivematica_failed_other_exception(self, create_package):
        exception_msg = "Error while archiving"
        create_package.side_effect = Exception(exception_msg)
        result = archivematica.apply(args=[self.step.id])

        result = result.get()
        self.step.refresh_from_db()

        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(self.step.output_data_json["status"], 1)
        self.assertIn(exception_msg, self.step.output_data_json["errormsg"])
        self.assertEqual(result["status"], 1)
        self.assertIn(exception_msg, result["errormsg"])
        self.assertEqual(self.step.step_type.current_count, 0)
        self.assertEqual(self.step.step_type.current_size_bytes, 0)

    def test_archivematica_file_size_exceeded(self):
        self.archive.sip_size = self.step.step_type.size_limit_bytes + 1
        self.archive.save()
        archivematica.apply(args=[self.step.id])

        self.step.refresh_from_db()
        msg = "SIP exceeds the Archivematica file size limit"
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertEqual(self.step.failure_type, StepFailureType.SIZE_EXCEEDED)
        self.assertIn(msg, self.step.output_data_json["errormsg"])
        self.assertEqual(self.step.step_type.current_count, 0)
        self.assertEqual(self.step.step_type.current_size_bytes, 0)

    def test_archivematica_aggr_file_size_exceeded(self):
        self._create_with_current_size(
            self.step.step_type.size_limit_bytes - self.archive.sip_size + 1
        )
        archivematica.apply(args=[self.step.id])

        self.step.refresh_from_db()
        msg = "Archivematica is busy"
        self.assertEqual(self.step.status, Status.WAITING)
        self.assertIn(msg, self.step.output_data_json["message"])
        self.assertEqual(self.step.step_type.current_count, 1)
        self.assertEqual(
            self.step.step_type.current_size_bytes,
            self.step.step_type.size_limit_bytes - self.archive.sip_size + 1,
        )

    def _create_with_current_size(self, size):
        archive = Archive.objects.create(recid="2", source="test_source", sip_size=size)
        Step.objects.create(
            archive=archive, step_name=StepName.ARCHIVE, status=Status.IN_PROGRESS
        )
