from django.contrib.auth.models import Permission, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.models import Archive, Status, Step, StepName


class StepViewTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="view_archive_all")
        self.execute_permission = Permission.objects.get(codename="can_execute_step")

        self.testuser = User.objects.create_user("testuser", password="pw")
        self.owner = User.objects.create_user("owner", password="pw")
        self.superuser = User.objects.create_superuser("admin", password="pw")

        self.archive = Archive.objects.create(
            recid="1",
            source="local",
            requester=self.owner,
            approver=self.superuser,
            title="",
        )

        self.harvest_step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.HARVEST,
            status=Status.COMPLETED,
        )

    def test_get_failure_types_empty(self):
        self.client.force_authenticate(user=self.superuser)

        response = self.client.get(reverse("steps-failure-types"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

    def test_get_failure_types(self):
        Step.objects.create(
            archive=self.archive,
            step_name=StepName.HARVEST,
            status=Status.FAILED,
            failure_type=StepFailureType.HTTP_403,
        )

        Step.objects.create(
            archive=self.archive,
            step_name=StepName.ARCHIVE,
            status=Status.FAILED,
            failure_type=StepFailureType.TIMEOUT,
        )

        Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            status=Status.FAILED,
            failure_type=StepFailureType.TIMEOUT,
        )

        Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            status=Status.FAILED,
            failure_type=StepFailureType.CONNECTION_ERROR,
        )

        self.client.force_authenticate(user=self.superuser)

        response = self.client.get(reverse("steps-failure-types"))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.data,
            [
                StepFailureType.CONNECTION_ERROR,
                StepFailureType.HTTP_403,
                StepFailureType.TIMEOUT,
            ],
        )
