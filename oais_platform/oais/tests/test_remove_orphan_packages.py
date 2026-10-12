import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase

from oais_platform.oais.models import Archive, ArchivematicaInstance
from oais_platform.oais.tasks.remove_orphan_packages import remove_orphan_packages


def _make_old(path, days=8):
    old_time = time.time() - (days * 24 * 3600)
    os.utime(path, (old_time, old_time))


class RemoveOrphanPackagesTests(TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

        self.sip_store_basepath = os.path.join(self.tmpdir.name, "sips")
        self.aip_basepath = os.path.join(self.tmpdir.name, "aips")
        os.makedirs(self.sip_store_basepath)
        os.makedirs(self.aip_basepath)

        self.sip_patch = patch(
            "oais_platform.oais.tasks.remove_orphan_packages.SIP_STORE_BASEPATH",
            self.sip_store_basepath,
        )
        self.sip_patch.start()

        self.dry_run_patch = patch(
            "oais_platform.oais.tasks.remove_orphan_packages.REMOVE_ORPHAN_PACKAGES_DRY_RUN",
            False,
        )
        self.dry_run_patch.start()

        ArchivematicaInstance.objects.create(
            name="AM1",
            url="http://am.example.com",
            username="test",
            api_key="test",
            storage_service_url="http://ss.example.com",
            storage_service_username="test",
            storage_service_api_key="test",
            sip_upstream_basepath=self.sip_store_basepath,
            aip_upstream_basepath=self.aip_basepath,
        )

    def tearDown(self):
        self.sip_patch.stop()
        self.dry_run_patch.stop()
        self.tmpdir.cleanup()

    def test_does_not_remove_known_aip(self):
        known_aip = Path(self.aip_basepath) / "known-123.7z"
        known_aip.write_bytes(b"real content")
        _make_old(known_aip)

        Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            path_to_sip="test_path",
            path_to_aip=str(known_aip),
        )

        report = remove_orphan_packages()

        self.assertTrue(known_aip.exists())
        self.assertEqual(report["aips"]["deleted"], 0)

    def test_does_not_remove_recent_orphan_aip(self):
        recent_aip = Path(self.aip_basepath) / "recent-orphan.7z"
        recent_aip.write_bytes(b"fake content")
        # left at current mtime -> not old enough to be removed

        report = remove_orphan_packages()

        self.assertTrue(recent_aip.exists())
        self.assertEqual(report["aips"]["deleted"], 0)

    def test_removes_orphan_aip_file(self):
        orphan_aip = Path(self.aip_basepath) / "orphan-123.7z"
        orphan_aip.write_bytes(b"fake content")
        _make_old(orphan_aip)

        report = remove_orphan_packages()

        self.assertFalse(orphan_aip.exists())
        self.assertEqual(report["aips"]["deleted"], 1)

    def test_removes_orphan_sip_folder(self):
        orphan_sip = Path(self.sip_store_basepath) / "sip::test::orphan::123"
        orphan_sip.mkdir()
        (orphan_sip / "some_file.txt").write_bytes(b"content")
        _make_old(orphan_sip)

        report = remove_orphan_packages()

        self.assertFalse(orphan_sip.exists())
        self.assertEqual(report["sips"]["deleted"], 1)

    def test_dry_run_does_not_delete_anything(self):
        self.dry_run_patch.stop()
        dry_run_true_patch = patch(
            "oais_platform.oais.tasks.remove_orphan_packages.REMOVE_ORPHAN_PACKAGES_DRY_RUN",
            True,
        )
        dry_run_true_patch.start()
        self.addCleanup(dry_run_true_patch.stop)

        orphan_aip = Path(self.aip_basepath) / "orphan-dry-run.7z"
        orphan_aip.write_bytes(b"fake content")
        _make_old(orphan_aip)

        report = remove_orphan_packages()

        self.assertTrue(orphan_aip.exists())
        self.assertEqual(report["aips"]["deleted"], 0)

    @patch("os.remove", side_effect=OSError("Permission denied"))
    def test_delete_error_is_captured_and_does_not_crash(self, mock_remove):
        orphan_aip = Path(self.aip_basepath) / "undeletable.7z"
        orphan_aip.write_bytes(b"fake content")
        _make_old(orphan_aip)

        report = remove_orphan_packages()

        self.assertEqual(report["aips"]["deleted"], 0)
        self.assertEqual(len(report["errors"]), 1)
