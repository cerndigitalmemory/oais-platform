from unittest.mock import patch

from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.archivematica import am_manager, start_am_transfers


class ArchivematicaManagerTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            path_to_sip="basepath/sips/test_path",
            sip_size=1000,
        )

        self.archive2 = Archive.objects.create(
            recid="2",
            source="test",
            source_url="",
            path_to_sip="basepath/sips/test_path2",
            sip_size=1000,
        )

        self.step = Step.objects.create(
            archive=self.archive, step_name=StepName.ARCHIVE, status=Status.WAITING
        )
        self.step2 = Step.objects.create(
            archive=self.archive2, step_name=StepName.ARCHIVE, status=Status.WAITING
        )
        self.step.step_type.size_limit_bytes = 2000
        self.step.step_type.concurrency_limit = 5
        self.step.step_type.save()

    @patch("oais_platform.oais.tasks.archivematica.chord")
    @patch("oais_platform.oais.tasks.archivematica.start_am_transfers.apply_async")
    def test_am_manager_no_steps(self, mock_start_transfers, mock_chord):
        am_manager.apply()
        mock_chord.assert_not_called()
        mock_start_transfers.assert_called_once()

    @patch("oais_platform.oais.tasks.archivematica.chord")
    def test_am_manager_with_steps(self, mock_chord):
        self.step.set_status(Status.SUBMITTED)
        self.step2.set_status(Status.IN_PROGRESS)
        am_manager.apply()
        chord_tasks = list(mock_chord.call_args[0][0])
        self.assertEqual(len(chord_tasks), 2)
        self.assertTrue(any(task.args[0] == self.step.id for task in chord_tasks))
        self.assertTrue(any(task.args[0] == self.step2.id for task in chord_tasks))

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_start_transfers_called(self, mock_archivematica):
        start_am_transfers.apply()
        mock_archivematica.assert_any_call(args=[self.step.id])
        mock_archivematica.assert_any_call(args=[self.step2.id])

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_start_transfers_not_called_for_non_waiting_steps(
        self, mock_archivematica
    ):
        self.step.set_status(Status.SUBMITTED)
        self.step2.set_status(Status.IN_PROGRESS)
        start_am_transfers.apply()
        mock_archivematica.assert_not_called()

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_start_transfer_concurrency_limit(self, mock_archivematica):
        self.step.step_type.concurrency_limit = 1
        self.step.step_type.save()
        self.step.set_status(Status.IN_PROGRESS)
        self.step.save()

        start_am_transfers.apply()
        self.step2.refresh_from_db()
        self.assertEqual(self.step2.status, Status.WAITING)
        mock_archivematica.assert_not_called()

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_start_transfer_step_disabled(self, mock_archivematica):
        self.step.step_type.enabled = False
        self.step.step_type.save()

        start_am_transfers.apply()
        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.WAITING)
        self.step2.refresh_from_db()
        self.assertEqual(self.step2.status, Status.WAITING)
        mock_archivematica.assert_not_called()
