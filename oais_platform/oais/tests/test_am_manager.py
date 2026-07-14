from datetime import timedelta
from unittest.mock import patch

from django.utils import timezone
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.archivematica import (
    am_manager,
    recover_stale_assigned_archivematica_steps,
    start_am_transfers,
)
from oais_platform.settings import AM_INSTANCES, AM_WAITING_TIME_LIMIT


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
        self.archive.set_last_step(self.step.id)
        self.step2 = Step.objects.create(
            archive=self.archive2, step_name=StepName.ARCHIVE, status=Status.WAITING
        )
        self.archive2.set_last_step(self.step2.id)
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
        mock_archivematica.return_value.id = "test-task-id"
        start_am_transfers.apply()
        mock_archivematica.assert_any_call(args=[self.step.id])
        mock_archivematica.assert_any_call(args=[self.step2.id])

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_start_transfers_not_called_for_non_waiting_steps(
        self, mock_archivematica
    ):
        mock_archivematica.return_value.id = "test-task-id"
        self.step.set_status(Status.SUBMITTED)
        self.step2.set_status(Status.IN_PROGRESS)
        start_am_transfers.apply()
        mock_archivematica.assert_not_called()

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_start_transfer_concurrency_limit(self, mock_archivematica):
        mock_archivematica.return_value.id = "test-task-id"
        self.step.step_type.concurrency_limit = 1
        self.step.step_type.save()
        self.step.set_input_data_field(
            "archivematica_instance", AM_INSTANCES[0]["AM_INSTANCE"]
        )
        self.step.set_status(Status.IN_PROGRESS)
        self.step2.set_input_data_field(
            "archivematica_instance", AM_INSTANCES[0]["AM_INSTANCE"]
        )

        start_am_transfers.apply()
        self.step2.refresh_from_db()
        self.assertEqual(self.step2.status, Status.WAITING)
        mock_archivematica.assert_not_called()

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_start_transfer_uses_instance_capacity(self, mock_archivematica):
        mock_archivematica.return_value.id = "test-task-id"
        am_instances = [
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM1"},
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM2"},
        ]
        self.step.step_type.concurrency_limit = 1
        self.step.step_type.save()
        self.step.set_input_data_field("archivematica_instance", "AM1")
        self.step.set_status(Status.IN_PROGRESS)
        self.step2.set_input_data_field("archivematica_instance", "AM2")

        with patch("oais_platform.oais.tasks.archivematica.AM_INSTANCES", am_instances):
            start_am_transfers.apply()

        mock_archivematica.assert_called_once_with(args=[self.step2.id])

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_assigns_unpinned_steps_to_highest_capacity_first(
        self, mock_archivematica
    ):
        am_instances = [
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM1"},
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM2"},
        ]
        archive3 = Archive.objects.create(
            recid="3",
            source="test",
            source_url="",
            path_to_sip="basepath/sips/test_path3",
            archivematica_instance=am_instances[0]["AM_INSTANCE"],
            sip_size=1000,
        )
        step3 = Step.objects.create(
            archive=archive3,
            step_name=StepName.ARCHIVE,
            status=Status.WAITING,
            input_data_json={"archivematica_instance": am_instances[0]["AM_INSTANCE"]},
        )
        archive3.set_last_step(step3.id)

        self.step.step_type.concurrency_limit = 2
        self.step.step_type.save()
        in_progress_step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.ARCHIVE,
            status=Status.IN_PROGRESS,
            input_data_json={"archivematica_instance": am_instances[0]["AM_INSTANCE"]},
        )
        self.archive.set_last_step(self.step.id)
        mock_archivematica.return_value.id = "test-task-id"
        with patch("oais_platform.oais.tasks.archivematica.AM_INSTANCES", am_instances):
            start_am_transfers.apply()

        self.step.refresh_from_db()
        self.step2.refresh_from_db()
        step3.refresh_from_db()
        self.assertEqual(self.step.input_data_json["archivematica_instance"], "AM2")
        self.assertEqual(self.step2.input_data_json["archivematica_instance"], "AM2")
        self.assertEqual(step3.input_data_json["archivematica_instance"], "AM1")
        self.assertEqual(mock_archivematica.call_count, 3)
        in_progress_step.refresh_from_db()
        self.assertEqual(in_progress_step.status, Status.IN_PROGRESS)

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_balances_400_waiting_archives(self, mock_archivematica):
        am_instances = [
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM1"},
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM2"},
        ]
        self.step.step_type.concurrency_limit = 33
        self.step.step_type.save()

        for index in range(3, 401):
            archive = Archive.objects.create(
                recid=str(index),
                source="test",
                source_url="",
                path_to_sip=f"basepath/sips/test_path{index}",
                sip_size=1000,
            )
            step = Step.objects.create(
                archive=archive,
                step_name=StepName.ARCHIVE,
                status=Status.WAITING,
            )
            archive.set_last_step(step.id)
        mock_archivematica.return_value.id = "test-task-id"
        with patch("oais_platform.oais.tasks.archivematica.AM_INSTANCES", am_instances):
            start_am_transfers.apply()

        assigned_steps = Step.objects.filter(
            step_type__name=StepName.ARCHIVE,
            input_data_json__has_key="archivematica_instance",
        )

        self.assertEqual(assigned_steps.count(), 66)
        self.assertEqual(
            assigned_steps.filter(
                input_data_json__archivematica_instance="AM1"
            ).count(),
            33,
        )
        self.assertEqual(
            assigned_steps.filter(
                input_data_json__archivematica_instance="AM2"
            ).count(),
            33,
        )
        self.assertEqual(mock_archivematica.call_count, 66)

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

    @patch("oais_platform.oais.tasks.archivematica.archivematica.apply_async")
    def test_am_manager_start_transfers_not_called_for_non_last_step(
        self, mock_archivematica
    ):
        mock_archivematica.return_value.id = "test-task-id"
        cta_step = Step.objects.create(
            archive=self.archive, step_name=StepName.PUSH_TO_CTA, status=Status.WAITING
        )
        self.archive.set_last_step(cta_step.id)
        start_am_transfers.apply()
        mock_archivematica.assert_called_once_with(args=[self.step2.id])

    @patch("oais_platform.oais.tasks.archivematica.app.control.revoke")
    def test_recover_stale_assigned_archivematica_steps_requeues_step(
        self, mock_revoke
    ):
        assigned_at = timezone.now() - timedelta(minutes=AM_WAITING_TIME_LIMIT + 1)
        self.step.set_input_data(
            {
                "archivematica_instance": AM_INSTANCES[0]["AM_INSTANCE"],
                "assigned_at": assigned_at.isoformat(),
            }
        )
        self.step.set_status(Status.ASSIGNED)
        self.step.set_task("stale-task-id")

        recover_stale_assigned_archivematica_steps()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.WAITING)
        self.assertIsNone(self.step.celery_task_id)
        self.assertNotIn("archivematica_instance", self.step.input_data_json)
        mock_revoke.assert_called_once_with("stale-task-id", terminate=True)

    @patch("oais_platform.oais.tasks.archivematica.app.control.revoke")
    def test_recover_stale_assigned_archivematica_steps_ignores_fresh_step(
        self, mock_revoke
    ):
        self.step.set_input_data(
            {
                "archivematica_instance": AM_INSTANCES[0]["AM_INSTANCE"],
                "assigned_at": timezone.now().isoformat(),
            }
        )
        self.step.set_status(Status.ASSIGNED)
        self.step.set_task("fresh-task-id")

        recover_stale_assigned_archivematica_steps()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.ASSIGNED)
        self.assertEqual(self.step.celery_task_id, "fresh-task-id")
        self.assertEqual(
            self.step.input_data_json["archivematica_instance"],
            AM_INSTANCES[0]["AM_INSTANCE"],
        )
        mock_revoke.assert_not_called()

    @patch("oais_platform.oais.tasks.archivematica.app.control.revoke")
    def test_recover_stale_assigned_archivematica_steps_ignores_started_step(
        self, mock_revoke
    ):
        assigned_at = timezone.now() - timedelta(minutes=AM_WAITING_TIME_LIMIT + 1)
        self.step.set_input_data(
            {
                "archivematica_instance": AM_INSTANCES[0]["AM_INSTANCE"],
                "assigned_at": assigned_at.isoformat(),
            }
        )
        self.step.set_status(Status.ASSIGNED)
        self.step.set_task("started-task-id")
        self.step.set_start_date()

        recover_stale_assigned_archivematica_steps()

        self.step.refresh_from_db()
        self.assertEqual(self.step.status, Status.ASSIGNED)
        self.assertEqual(self.step.celery_task_id, "started-task-id")
        mock_revoke.assert_not_called()
