import errno
from unittest.mock import MagicMock, Mock, patch

from django.apps import apps
from django.utils import timezone
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.cta import push_to_cta
from oais_platform.settings import CTA_BASE_PATH, FTS_SOURCE_BASE_PATH


class MockedGError(Exception):
    def __init__(self, message, code):
        super().__init__(message)
        self.message = message
        self.code = code


@patch("oais_platform.oais.tasks.cta.gfal2")
class PushToCTATests(APITestCase):
    MockedGError = MockedGError

    def setUp(self):
        self.app_config = apps.get_app_config("oais")
        self.fts = MagicMock()
        self.app_config.fts = self.fts

        path_to_aip = "basepath/aips/test/path/filename.zip"
        self.archive = Archive.objects.create(path_to_aip=path_to_aip)
        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            start_date=timezone.now(),
        )
        self.step.set_input_data({"test": "test"})

        self.expected_source = f"{FTS_SOURCE_BASE_PATH}/{path_to_aip}"
        self.expected_destination = f"{CTA_BASE_PATH}aips/test/path/filename.zip"

        self.path_patch = patch(
            "oais_platform.oais.tasks.cta.AIP_UPSTREAM_BASEPATH", "basepath/aips"
        )
        self.path_patch.start()

    def tearDown(self):
        self.path_patch.stop()

    def _setup_gfal2_mocks(self, mock_gfal2, error=True):
        mock_ctx = Mock()
        if error:
            mock_ctx.stat.side_effect = self.MockedGError(
                "404 File not found", errno.ENOENT
            )
            mock_gfal2.GError = self.MockedGError
        else:
            mock_ctx.stat.return_value = Mock(st_size=123456)
            mock_ctx.checksum.return_value = "test-checksum"
        mock_gfal2.creat_context.return_value = mock_ctx

    def test_push_to_cta_success(self, mock_gfal2):
        self._setup_gfal2_mocks(mock_gfal2)
        self.fts.push_to_cta.return_value = "test_job_id"
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.step.refresh_from_db()
        self.assertEqual(self.fts.push_to_cta.call_count, 1)
        self.fts.push_to_cta.assert_called_once_with(
            self.expected_source,
            self.expected_destination,
            True,
        )
        self.assertEqual(self.step.status, Status.IN_PROGRESS)
        self.assertEqual(self.step.get_output_data()["fts_job_id"], "test_job_id")

    @patch("oais_platform.oais.tasks.cta.create_retry_step.apply_async")
    def test_push_to_cta_exception(self, mock_retry_step, mock_gfal2):
        self._setup_gfal2_mocks(mock_gfal2)
        self.fts.push_to_cta.side_effect = Exception("FTS Service Down")
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.step.refresh_from_db()
        self.assertEqual(self.fts.push_to_cta.call_count, 1)
        self.assertEqual(self.step.status, Status.FAILED)
        self.assertIsNotNone(self.step.finish_date)
        output_data = self.step.get_output_data()
        self.assertTrue(output_data["retrying"])
        self.assertEqual(output_data["msg"], "FTS Service Down")
        mock_retry_step.assert_called_once()

    @patch("oais_platform.oais.tasks.cta.Path.stat")
    @patch("oais_platform.oais.tasks.cta.compute_hash")
    def test_push_to_cta_file_exists_on_tape(
        self, mock_checksum, mock_stat, mock_gfal2
    ):
        self._setup_gfal2_mocks(mock_gfal2, False)
        mock_stat.return_value.st_size = 123456
        mock_checksum.return_value = "test-checksum"
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.step.refresh_from_db()
        self.assertEqual(self.fts.push_to_cta.call_count, 0)
        self.assertEqual(self.step.status, Status.COMPLETED)
        self.assertIsNotNone(self.step.finish_date)

    @patch("oais_platform.oais.tasks.cta.Path.stat")
    def test_push_to_cta_file_exists_on_tape_different_size(
        self, mock_stat, mock_gfal2
    ):
        self._setup_gfal2_mocks(mock_gfal2, error=False)
        mock_stat.return_value.st_size = 100
        self.fts.push_to_cta.return_value = "test_job_id"
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.step.refresh_from_db()
        self.assertEqual(self.fts.push_to_cta.call_count, 1)
        self.fts.push_to_cta.assert_called_once_with(
            self.expected_source,
            self.expected_destination,
            True,
        )
        self.assertEqual(self.step.status, Status.IN_PROGRESS)

    @patch("oais_platform.oais.tasks.cta.Path.stat")
    @patch("oais_platform.oais.tasks.cta.compute_hash")
    def test_push_to_cta_file_exists_on_tape_different_checksum(
        self, mock_checksum, mock_stat, mock_gfal2
    ):
        self._setup_gfal2_mocks(mock_gfal2, error=False)
        mock_stat.return_value.st_size = 123456
        mock_checksum.return_value = "mismatching-checksum"
        self.fts.push_to_cta.return_value = "test_job_id"
        push_to_cta.apply(args=[self.archive.id, self.step.id])
        self.step.refresh_from_db()
        self.assertEqual(self.fts.push_to_cta.call_count, 1)
        self.fts.push_to_cta.assert_called_once_with(
            self.expected_source,
            self.expected_destination,
            True,
        )
        self.assertEqual(self.step.status, Status.IN_PROGRESS)
