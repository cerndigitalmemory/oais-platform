from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName


class ArchiveActionIntersectionTest(APITestCase):
    def create_archive_with_state(self, last_step_status, pipeline_steps=None):
        steps = pipeline_steps if pipeline_steps is not None else [123]

        archive = Archive.objects.create(pipeline_steps=steps)
        step = Step.objects.create(
            archive=archive, step_name=StepName.HARVEST, status=last_step_status
        )
        archive.set_last_step(step.id)
        return archive

    def setUp(self):
        self.user = User.objects.create_superuser("user", "", "pw")
        self.client.force_authenticate(user=self.user)
        self.url = reverse("archives-actions")

    def test_all_failed_and_can_continue(self):
        archive1 = self.create_archive_with_state(Status.FAILED)
        archive2 = self.create_archive_with_state(Status.FAILED)

        payload = {"archives": [archive1.id, archive2.id]}
        response = self.client.post(self.url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["all_last_step_failed"])
        self.assertTrue(response.data["can_continue"])

    def test_mixed_failed_and_warning(self):
        archive1 = self.create_archive_with_state(Status.FAILED)
        archive2 = self.create_archive_with_state(Status.COMPLETED_WITH_WARNINGS)

        payload = {"archives": [archive1.id, archive2.id]}
        response = self.client.post(self.url, payload, format="json")

        self.assertFalse(response.data["all_last_step_failed"])
        self.assertTrue(response.data["can_continue"])

    def test_cannot_continue_empty_pipeline(self):
        archive = self.create_archive_with_state(Status.FAILED, pipeline_steps=[])

        payload = {"archives": [archive.id]}
        response = self.client.post(self.url, payload, format="json")

        self.assertTrue(response.data["all_last_step_failed"])
        self.assertFalse(response.data["can_continue"])

    def test_cannot_continue_with_successful_step(self):
        archive = self.create_archive_with_state(Status.COMPLETED)

        payload = {"archives": [archive.id]}
        response = self.client.post(self.url, payload, format="json")

        self.assertFalse(response.data["all_last_step_failed"])
        self.assertFalse(response.data["can_continue"])

    def test_missing_last_step_logic(self):
        archive = Archive.objects.create()

        payload = {"archives": [archive.id]}
        response = self.client.post(self.url, payload, format="json")

        self.assertFalse(response.data["all_last_step_failed"])
        self.assertFalse(response.data["can_continue"])

    def test_empty_input_list(self):
        response = self.client.post(self.url, {"archives": []}, format="json")
        self.assertEqual(response.data, {})
