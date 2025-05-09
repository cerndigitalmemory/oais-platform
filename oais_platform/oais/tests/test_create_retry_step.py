import json

from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.tasks import create_retry_step


class CreateRetryStepTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create()
        self.step = Step.objects.create(
            archive=self.archive,
            name=Steps.PUSH_TO_CTA,
            status=Status.FAILED,
            output_data=json.dumps({"test": True}),
        )
        self.archive.set_last_step(self.step.id)

    def test_create_retry_step_success(self):
        create_retry_step.apply(args=[self.archive.id])
        retry_step = Step.objects.filter(
            name=self.step.name, archive=self.archive, input_step_id=self.step.id
        ).first()
        self.assertIsNotNone(retry_step)
        self.archive.refresh_from_db()
        self.assertEqual(self.archive.pipeline_steps, [retry_step.id])

    def test_create_retry_step_not_failed(self):
        self.step.set_status(Status.COMPLETED)
        create_retry_step.apply(args=[self.archive.id])
        self.assertFalse(
            Step.objects.filter(
                name=self.step.name, archive=self.archive, input_step_id=self.step.id
            ).exists()
        )

    def test_create_retry_step_name_mismatch(self):
        create_retry_step.apply(
            args=[self.archive.id], kwargs={"step_name": Steps.ARCHIVE}
        )
        self.assertFalse(
            Step.objects.filter(
                name=self.step.name, archive=self.archive, input_step_id=self.step.id
            ).exists()
        )
