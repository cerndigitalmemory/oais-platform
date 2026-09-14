from unittest.mock import Mock, patch

from django.test import TestCase
from django.utils import timezone
from requests.exceptions import RequestException

from oais_platform.oais.tasks.archivematica import refresh_am_instance_versions
from oais_platform.oais.tests.am_utils import (
    AM_INSTANCES,
    create_archivematica_instance,
)


class RefreshAmInstanceVersionsTests(TestCase):
    @patch("oais_platform.oais.tasks.archivematica.requests.get")
    def test_version_and_checked_at_are_updated_on_success(self, mock_get):
        instance = create_archivematica_instance()
        mock_get.return_value = Mock(headers={"X-Archivematica-Version": "1.18.0"})

        before = timezone.now()
        refresh_am_instance_versions()

        instance.refresh_from_db()
        self.assertEqual(instance.version, "1.18.0")
        self.assertGreaterEqual(instance.version_checked_at, before)

    @patch("oais_platform.oais.tasks.archivematica.requests.get")
    def test_disabled_instances_are_skipped(self, mock_get):
        instance = create_archivematica_instance()
        instance.enabled = False
        instance.save()

        refresh_am_instance_versions()

        mock_get.assert_not_called()
        instance.refresh_from_db()
        self.assertIsNone(instance.version)

    @patch("oais_platform.oais.tasks.archivematica.requests.get")
    def test_request_failure_does_not_crash_or_update_version(self, mock_get):
        instance = create_archivematica_instance()
        mock_get.side_effect = RequestException("Connection refused")

        refresh_am_instance_versions()  # should not raise

        instance.refresh_from_db()
        self.assertIsNone(instance.version)
        self.assertIsNone(instance.version_checked_at)

    @patch("oais_platform.oais.tasks.archivematica.requests.get")
    def test_missing_header_does_not_update_version(self, mock_get):
        instance = create_archivematica_instance()
        mock_get.return_value = Mock(headers={})

        refresh_am_instance_versions()

        instance.refresh_from_db()
        self.assertIsNone(instance.version)

    @patch("oais_platform.oais.tasks.archivematica.requests.get")
    def test_all_enabled_instances_are_refreshed_independently(self, mock_get):
        first_instance = create_archivematica_instance()
        second_instance = create_archivematica_instance(
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM2"}
        )
        mock_get.return_value = Mock(headers={"X-Archivematica-Version": "1.19.0"})

        refresh_am_instance_versions()

        first_instance.refresh_from_db()
        second_instance.refresh_from_db()
        self.assertEqual(first_instance.version, "1.19.0")
        self.assertEqual(second_instance.version, "1.19.0")
        self.assertEqual(mock_get.call_count, 2)
