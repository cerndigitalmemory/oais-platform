import os
import shutil
import tempfile

from django.contrib.auth.models import User
from rest_framework.test import APITestCase

from oais_platform.oais.enums import Status, StepName
from oais_platform.oais.models import (
    Archive,
    ArchivematicaInstance,
    Status,
    Step,
    StepName,
)
from oais_platform.oais.tasks.archivematica import (
    _delete_aip,
    outdate_previous_archive_steps,
)


class OutdatePreviousArchiveStepsTests(APITestCase):
    """Tests for outdating previous Archive steps and deleting their AIP files."""

    def setUp(self):
        self.requester = User.objects.create_user("requester", password="pw")
        self.archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.requester,
        )
        self.temp_dir = tempfile.mkdtemp()
        self.aip_basepath = os.path.join(self.temp_dir, "aips", "am1")
        os.makedirs(self.aip_basepath)
        self.am_instance = ArchivematicaInstance.objects.create(
            name="test-instance",
            url="https://archivematica.example",
            username="user",
            _api_key="key",
            storage_service_url="https://storage.example",
            storage_service_username="user",
            _storage_service_api_key="key",
            sip_upstream_basepath=os.path.join(self.temp_dir, "sips"),
            aip_upstream_basepath=self.aip_basepath,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def make_aip_file(self, relative_path):
        aip_path = os.path.join(self.aip_basepath, relative_path)
        os.makedirs(os.path.dirname(aip_path), exist_ok=True)
        with open(aip_path, "w") as f:
            f.write("dummy content")
        return aip_path

    def make_archive_step(self, status, aip_path=None):
        output_data = {}
        if aip_path:
            output_data = {
                "artifact": {
                    "artifact_name": "AIP",
                    "artifact_path": aip_path,
                }
            }
        return Step.objects.create(
            archive=self.archive,
            step_name=StepName.ARCHIVE,
            status=status,
            input_data_json={"archivematica_instance": self.am_instance.name},
            output_data_json=output_data,
        )

    def test_outdates_old_step_and_deletes_its_aip(self):
        old_aip = self.make_aip_file("aaaa/bbbb/old.7z")
        new_aip = self.make_aip_file("cccc/dddd/new.7z")
        old_step = self.make_archive_step(Status.COMPLETED, old_aip)
        new_step = self.make_archive_step(Status.COMPLETED, new_aip)

        outdate_previous_archive_steps(self.archive, new_step, new_aip)

        old_step.refresh_from_db()
        new_step.refresh_from_db()
        self.assertEqual(old_step.status, Status.OUTDATED)
        self.assertIn("outdated_at", old_step.output_data_json)
        self.assertIn("aip_deleted_at", old_step.output_data_json)
        self.assertFalse(os.path.exists(old_aip))
        # empty parent folders are removed, the AIP basepath is kept
        self.assertFalse(os.path.exists(os.path.join(self.aip_basepath, "aaaa")))
        self.assertTrue(os.path.isdir(self.aip_basepath))
        # the new step and its AIP are untouched
        self.assertEqual(new_step.status, Status.COMPLETED)
        self.assertTrue(os.path.exists(new_aip))

    def test_does_not_delete_aip_when_path_equals_new_aip(self):
        shared_aip = self.make_aip_file("aaaa/bbbb/shared.7z")
        old_step = self.make_archive_step(Status.COMPLETED, shared_aip)
        new_step = self.make_archive_step(Status.COMPLETED, shared_aip)

        outdate_previous_archive_steps(self.archive, new_step, shared_aip)

        old_step.refresh_from_db()
        self.assertTrue(os.path.exists(shared_aip))
        self.assertNotIn("aip_deleted_at", old_step.output_data_json)

    def test_missing_aip_file_does_not_raise(self):
        missing_aip = os.path.join(self.aip_basepath, "aaaa", "missing.7z")
        new_aip = self.make_aip_file("cccc/dddd/new.7z")
        old_step = self.make_archive_step(Status.COMPLETED, missing_aip)
        new_step = self.make_archive_step(Status.COMPLETED, new_aip)

        outdate_previous_archive_steps(self.archive, new_step, new_aip)

        old_step.refresh_from_db()
        self.assertEqual(old_step.status, Status.OUTDATED)
        self.assertNotIn("aip_deleted_at", old_step.output_data_json)
        self.assertTrue(os.path.exists(new_aip))

    def test_failed_previous_step_is_not_outdated(self):
        new_aip = self.make_aip_file("cccc/dddd/new.7z")
        failed_step = self.make_archive_step(Status.FAILED)
        new_step = self.make_archive_step(Status.COMPLETED, new_aip)

        outdate_previous_archive_steps(self.archive, new_step, new_aip)

        failed_step.refresh_from_db()
        self.assertEqual(failed_step.status, Status.FAILED)
        self.assertTrue(os.path.exists(new_aip))

    def test_delete_aip_refuses_path_outside_basepath(self):
        outside_path = os.path.join(self.temp_dir, "sips", "important.7z")
        os.makedirs(os.path.dirname(outside_path))
        with open(outside_path, "w") as f:
            f.write("do not delete")
        step = self.make_archive_step(Status.COMPLETED, outside_path)

        result = _delete_aip(step, outside_path, self.aip_basepath)

        self.assertFalse(result)
        self.assertTrue(os.path.exists(outside_path))
