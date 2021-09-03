from unittest.mock import patch

from django.contrib.auth.models import Permission, User
from django.urls import reverse
from oais_platform.oais.models import Archive, Status, Record
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase


class ApprovalTests(APITestCase):
    def setUp(self):
        self.reject_permission = Permission.objects.get(
            codename="can_reject_archive")
        self.approve_permission = Permission.objects.get(
            codename="can_approve_archive")
        self.access_permission = Permission.objects.get(
            codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")
        self.client.force_authenticate(user=self.creator)

        self.record = Record.objects.create(recid="1", source="test", url="")
        self.archive = Archive.objects.create(
            record=self.record, creator=self.creator)

    def test_reject_not_authenticated(self):
        self.client.force_authenticate(user=None)

        url = reverse("archive-reject", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.archive.status, Status.WAITING_APPROVAL)

    def test_reject_without_permission(self):
        url = reverse("archive-reject", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.archive.status, Status.WAITING_APPROVAL)

    def test_reject_with_permission(self):
        self.creator.user_permissions.add(self.reject_permission)
        self.creator.save()

        url = reverse("archive-reject", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.status, Status.REJECTED)

    def test_approve_not_authenticated(self):
        self.client.force_authenticate(user=None)

        url = reverse("archive-approve", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.archive.status, Status.WAITING_APPROVAL)

    def test_approve_without_permission(self):
        url = reverse("archive-approve", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.archive.status, Status.WAITING_APPROVAL)

    @patch("oais_platform.oais.tasks.process.delay")
    def test_approve_with_permission(self, process_delay):
        self.creator.user_permissions.add(self.approve_permission)
        self.creator.save()

        url = reverse("archive-approve", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.status, Status.PENDING)
        process_delay.assert_called_once_with(self.archive.id)

    def test_reject_not_waiting_approval(self):
        self.creator.user_permissions.add(self.reject_permission)
        self.creator.save()

        url = reverse("archive-reject", args=[self.archive.id])
        for archive_status in Status.values:
            if archive_status == Status.WAITING_APPROVAL:
                continue
            self.archive.status = archive_status
            self.archive.save()

            response = self.client.post(url, format="json")

            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
            self.assertEqual(
                response.data["detail"], "Archive is not waiting for approval")

            self.archive.refresh_from_db()
            self.assertEqual(self.archive.status, archive_status)

    def test_reject_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.reject_permission)
        self.other_user.user_permissions.add(self.access_permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archive-reject", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.status, Status.REJECTED)

    @patch("oais_platform.oais.tasks.process.delay")
    def test_approve_other_user_with_perm(self, process_delay):
        self.other_user.user_permissions.add(self.approve_permission)
        self.other_user.user_permissions.add(self.access_permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archive-approve", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.status, Status.PENDING)
        process_delay.assert_called_once_with(self.archive.id)

    def test_reject_other_user_without_access_perm(self):
        self.other_user.user_permissions.add(self.reject_permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archive-reject", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(self.archive.status, Status.WAITING_APPROVAL)

    def test_approve_other_user_without_access_perm(self):
        self.other_user.user_permissions.add(self.approve_permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archive-approve", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(self.archive.status, Status.WAITING_APPROVAL)