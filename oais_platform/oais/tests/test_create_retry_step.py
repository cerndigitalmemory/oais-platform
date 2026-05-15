from unittest.mock import patch

from django.contrib.auth.models import User
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.pipeline_actions import create_retry_step


class CreateRetryStepTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create()
        self.step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            status=Status.FAILED,
            output_data_json={"test": True},
        )
        self.archive.set_last_step(self.step.id)
        self.user = User.objects.create_superuser("user", "", "pw")

    @patch("oais_platform.oais.tasks.pipeline_actions.run_step")
    def test_create_retry_step_success(self, mock_run_step):
        create_retry_step.apply(args=[self.archive.id, self.user.id, True])
        retry_step = Step.objects.filter(
            step_type=self.step.step_type,
            archive=self.archive,
            input_step_id=self.step.id,
        ).first()
        self.assertIsNotNone(retry_step)
        self.archive.refresh_from_db()
        self.assertEqual(retry_step.initiated_by_user, self.user)
        self.assertEqual(retry_step.initiated_by_harvest_batch, None)
        mock_run_step.assert_called_once_with(retry_step, self.archive.id)

    def test_create_retry_step_not_failed(self):
        self.step.set_status(Status.COMPLETED)
        create_retry_step.apply(args=[self.archive.id])
        self.assertFalse(
            Step.objects.filter(
                step_type=self.step.step_type,
                archive=self.archive,
                input_step_id=self.step.id,
            ).exists()
        )

    def test_create_retry_step_name_mismatch(self):
        create_retry_step.apply(
            args=[self.archive.id], kwargs={"step_name": StepName.ARCHIVE}
        )
        self.assertFalse(
            Step.objects.filter(
                step_type=self.step.step_type,
                archive=self.archive,
                input_step_id=self.step.id,
            ).exists()
        )
