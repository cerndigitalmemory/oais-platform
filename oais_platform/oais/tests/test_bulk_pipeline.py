from unittest.mock import MagicMock, patch

from django.contrib.auth.models import Permission, User
from django.urls import reverse
from parameterized import parameterized
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    ArchiveState,
    Source,
    Status,
    Step,
    StepName,
    StepType,
)
from oais_platform.oais.tasks.pipeline_actions import run_bulk_pipeline
from oais_platform.settings import PIPELINE_SIZE_LIMIT


class BulkPipelineTests(APITestCase):
    def setUp(self):
        self.view_permission = Permission.objects.get(codename="view_archive_all")
        self.execute_permission = Permission.objects.get(codename="can_execute_step")
        # self.edit_permission = Permission.objects.get(codename="can_edit_all")

        self.testuser = User.objects.create_user("testuser", password="pw")
        self.testuser.user_permissions.add(
            self.execute_permission
        )  # self.view_permission
        self.testuser.save()

        self.source = Source.objects.create(name="test", classname="Local")
        self.testuser_api_key = ApiKey.objects.create(
            user=self.testuser, source=self.source, key="abcd1234"
        )

        self.archives = [
            Archive.objects.create(
                recid=str(i),
                source=self.source.name,
                requester=self.testuser,
                state=ArchiveState.SIP,
            )
            for i in range(3)
        ]
        self.archive_ids = [a.id for a in self.archives]
        self.url = reverse("archives-bulk-pipeline")

        for archive in self.archives:
            harvest_step = Step.objects.create(
                archive=archive, step_name=StepName.HARVEST
            )
            archive.set_last_completed_step(harvest_step.id)
            archive.set_last_step(harvest_step.id)

            Step.objects.create(archive=archive, step_name=StepName.VALIDATION)

        self.init_step_count = Step.objects.count()

    @parameterized.expand(
        [
            ([], [StepName.VALIDATION], "run", status.HTTP_400_BAD_REQUEST),
            ([1, 2], [], "run", status.HTTP_400_BAD_REQUEST),
            (
                [1, 2],
                [i for i in range(PIPELINE_SIZE_LIMIT + 1)],
                "run",
                status.HTTP_400_BAD_REQUEST,
            ),
            (
                [1, 2],
                [StepName.VALIDATION],
                "invalid-type",
                status.HTTP_400_BAD_REQUEST,
            ),
        ]
    )
    def test_bulk_pipeline_invalid_input(self, ids, steps, run_type, status_code):
        self.client.force_authenticate(user=self.testuser)

        response = self.client.post(
            self.url,
            {"archive_ids": ids, "pipeline_steps": steps, "run_type": run_type},
            format="json",
        )

        self.assertEqual(response.status_code, status_code)
        self.assertEqual(Step.objects.count(), self.init_step_count)

    @patch("oais_platform.oais.tasks.pipeline_actions.run_bulk_pipeline.delay")
    def test_bulk_pipeline_task_creation(self, mock_delay):
        self.client.force_authenticate(user=self.testuser)

        payload = {
            "archive_ids": self.archive_ids,
            "pipeline_steps": [StepName.ARCHIVE],
            "run_type": "run",
        }
        response = self.client.post(self.url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data["msg"], f"Processing {len(self.archive_ids)} archives"
        )

        mock_delay.assert_called_once_with(
            self.archive_ids, "run", [StepName.ARCHIVE], self.testuser.id
        )

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_run_bulk_pipeline_task_logic(self, mock_dispatch):
        pipeline = [StepName.ARCHIVE, StepName.PUSH_TO_CTA, StepName.INVENIO_RDM_PUSH]
        run_bulk_pipeline.apply(
            args=[self.archive_ids, "run", pipeline, self.testuser.id]
        )

        steps_created = (
            Step.objects.filter(archive_id__in=self.archive_ids).count()
            - self.init_step_count
        )
        self.assertEqual(steps_created, len(self.archive_ids) * len(pipeline))
        self.assertEqual(mock_dispatch.call_count, len(self.archive_ids))

        for archive in self.archives:
            archive.refresh_from_db()
            mock_dispatch.assert_any_call(
                StepType.objects.filter(name=StepName.ARCHIVE).first(),
                archive.id,
                archive.last_step.id,
                False,
            )

    @patch("oais_platform.oais.tasks.pipeline_actions.execute_pipeline")
    def test_bulk_retry_logic(self, mock_execute):
        mock_execute.return_value = (MagicMock(spec=Step), None)
        for archive in self.archives:
            step = Step.objects.create(
                archive=archive, step_name=StepName.PUSH_TO_CTA, status=Status.FAILED
            )
            archive.set_last_step(step.id)

        run_bulk_pipeline.apply(args=[self.archive_ids, "retry", [], self.testuser.id])

        self.assertEqual(mock_execute.call_count, len(self.archive_ids))
        for archive_id in self.archive_ids:
            mock_execute.assert_any_call(archive_id, force_continue=True)

    @patch("oais_platform.oais.tasks.pipeline_actions.execute_pipeline")
    def test_bulk_continue_logic(self, mock_execute):
        mock_execute.return_value = (MagicMock(spec=Step), None)
        for archive in self.archives:
            failed_step = Step.objects.create(
                archive=archive, step_name=StepName.HARVEST, status=Status.FAILED
            )
            waiting_step = Step.objects.create(
                archive=archive, step_name=StepName.VALIDATION, status=Status.WAITING
            )

            archive.set_last_step(failed_step.id)
            archive.pipeline_steps = [waiting_step.id]
            archive.save()

        run_bulk_pipeline.apply(
            args=[self.archive_ids, "continue", [], self.testuser.id]
        )

        self.assertEqual(mock_execute.call_count, len(self.archive_ids))
        for archive_id in self.archive_ids:
            mock_execute.assert_any_call(archive_id, force_continue=True)

    @patch("oais_platform.oais.tasks.pipeline_actions.execute_pipeline")
    def test_bulk_continue_logic_invalid_state(self, mock_execute):
        for archive in self.archives:
            completed_step = Step.objects.create(
                archive=archive, step_name=StepName.HARVEST, status=Status.COMPLETED
            )
            archive.set_last_step(completed_step.id)
            archive.save()

        with self.assertLogs(level="WARNING") as mock_logs:
            run_bulk_pipeline.apply(
                args=[self.archive_ids, "continue", [], self.testuser.id]
            )
            self.assertTrue(
                any(
                    "Continue operation not permitted" in msg
                    for msg in mock_logs.output
                )
            )

        mock_execute.assert_not_called()

    @patch("oais_platform.oais.tasks.pipeline_actions.execute_pipeline")
    def test_run_bulk_pipeline_task_invalid_type(self, mock_execute):
        invalid_type = "unsupported_operation"

        with self.assertLogs(level="WARNING") as mock_logs:
            run_bulk_pipeline.apply(
                args=[self.archive_ids, invalid_type, [], self.testuser.id]
            )
            self.assertTrue(
                any(
                    "Invalid run_type param, possible values: ('run', 'retry', 'continue')."
                    in msg
                    for msg in mock_logs.output
                )
            )

        mock_execute.assert_not_called()
