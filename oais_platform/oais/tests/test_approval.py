from unittest.mock import patch

from django.contrib.auth.models import Permission, User
from django.urls import reverse
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, Steps
from oais_platform.oais.serializers import ProfileSerializer


class ApprovalTests(APITestCase):
    def setUp(self):
        self.reject_permission = Permission.objects.get(codename="can_reject_archive")
        self.approve_permission = Permission.objects.get(codename="can_approve_archive")
        self.access_permission = Permission.objects.get(
            codename="can_access_all_archives"
        )

        self.creator = User.objects.create_user("creator", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")
        self.client.force_authenticate(user=self.creator)

        self.archive = Archive.objects.create(
            creator=self.creator, recid="1", source="test", source_url=""
        )
        self.step = Step.objects.create(
            archive=self.archive, name=Steps.HARVEST, status=Status.WAITING_APPROVAL
        )

    def test_reject_not_authenticated(self):
        self.client.force_authenticate(user=None)

        url = reverse("steps-reject", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.step.status, Status.WAITING_APPROVAL)

    def test_reject_without_permission(self):
        url = reverse("steps-reject", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.step.status, Status.WAITING_APPROVAL)

    def test_reject_with_permission(self):
        self.creator.user_permissions.add(self.reject_permission)
        self.creator.save()

        url = reverse("steps-reject", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.step.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.step.status, Status.REJECTED)

    def test_approve_not_authenticated(self):
        self.client.force_authenticate(user=None)

        url = reverse("steps-approve", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.step.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.step.status, Status.WAITING_APPROVAL)

    def test_approve_without_permission(self):
        url = reverse("steps-approve", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.step.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.step.status, Status.WAITING_APPROVAL)

    @patch("oais_platform.oais.tasks.process.delay")
    def test_approve_with_permission(self, process_delay):
        self.creator.user_permissions.add(self.approve_permission)
        self.creator.save()

        url = reverse("steps-approve", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.step.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.step.status, Status.NOT_RUN)
        process_delay.assert_called_once_with(
            self.archive.id, self.step.id, input_data=None, api_key=None
        )

    def test_reject_not_waiting_approval(self):
        self.creator.user_permissions.add(self.reject_permission)
        self.creator.save()

        url = reverse("steps-reject", args=[self.step.id])
        for step_status in Status.values:
            if step_status == Status.WAITING_APPROVAL:
                continue
            self.step.status = step_status
            self.step.save()

            response = self.client.post(url, format="json")

            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
            self.assertEqual(
                response.data["detail"], "Archive is not waiting for approval"
            )

            self.step.refresh_from_db()
            self.assertEqual(self.step.status, step_status)

    def test_reject_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.reject_permission)
        self.other_user.user_permissions.add(self.access_permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("steps-reject", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.step.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.step.status, Status.REJECTED)

    @patch("oais_platform.oais.tasks.process.delay")
    def test_approve_other_user_with_perm(self, process_delay):
        self.other_user.user_permissions.add(self.approve_permission)
        self.other_user.user_permissions.add(self.access_permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("steps-approve", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.step.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.step.status, Status.NOT_RUN)
        process_delay.assert_called_once_with(
            self.archive.id, self.step.id, input_data=None, api_key=None
        )

    def test_reject_other_user_without_access_perm(self):
        self.other_user.user_permissions.add(self.reject_permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("steps-reject", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.step.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(self.step.status, Status.WAITING_APPROVAL)

    def test_approve_other_user_without_access_perm(self):
        self.other_user.user_permissions.add(self.approve_permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("steps-approve", args=[self.step.id])
        response = self.client.post(url, format="json")

        self.step.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(self.step.status, Status.WAITING_APPROVAL)
