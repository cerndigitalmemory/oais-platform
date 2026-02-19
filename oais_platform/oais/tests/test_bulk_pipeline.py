from unittest.mock import patch

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
            ([], [StepName.VALIDATION], status.HTTP_400_BAD_REQUEST),
            ([1, 2], [], status.HTTP_400_BAD_REQUEST),
            (
                [1, 2],
                [i for i in range(PIPELINE_SIZE_LIMIT + 1)],
                status.HTTP_400_BAD_REQUEST,
            ),
        ]
    )
    def test_bulk_pipeline_invalid_input(self, ids, steps, status_code):
        self.client.force_authenticate(user=self.testuser)

        response = self.client.post(
            self.url,
            {"archive_ids": ids, "pipeline_steps": steps, "run_type": "run"},
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
                None,
                self.testuser_api_key.key,
                False,
            )

    @patch("oais_platform.oais.tasks.pipeline_actions.execute_pipeline")
    def test_bulk_retry_logic(self, mock_execute):
        for archive in self.archives:
            step = Step.objects.create(
                archive=archive, step_name=StepName.PUSH_TO_CTA, status=Status.FAILED
            )
            archive.set_last_step(step.id)

        run_bulk_pipeline.apply(args=[self.archive_ids, "retry", [], self.testuser.id])

        self.assertEqual(mock_execute.call_count, len(self.archive_ids))
        for archive_id in self.archive_ids:
            mock_execute.assert_any_call(
                archive_id, api_key=self.testuser_api_key.key, force_continue=True
            )
