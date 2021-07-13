from unittest.mock import patch

from django.contrib.auth.models import Permission, User
from django.urls import reverse
from oais_platform.oais.models import Archive, ArchiveStatus, Record
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase


class ApprovalTests(APITestCase):
    def setUp(self):
        self.reject_permission = Permission.objects.get(
            codename="can_reject_archive")
        self.approve_permission = Permission.objects.get(
            codename="can_approve_archive")

        self.user = User.objects.create_user("user", "", "pw")
        self.token = Token.objects.create(user=self.user)
        self.client.credentials(HTTP_AUTHORIZATION="Token " + self.token.key)

        self.record = Record.objects.create(recid="1", source="test", url="")
        self.archive = Archive.objects.create(
            record=self.record, creator=self.user)

    def test_reject_not_authenticated(self):
        self.client.credentials()

        url = reverse("archive-reject", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(self.archive.status, ArchiveStatus.WAITING_APPROVAL)

    def test_reject_without_permission(self):
        url = reverse("archive-reject", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.archive.status, ArchiveStatus.WAITING_APPROVAL)

    def test_reject_with_permission(self):
        self.user.user_permissions.add(self.reject_permission)
        self.user.save()

        url = reverse("archive-reject", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.status, ArchiveStatus.REJECTED)

    def test_approve_not_authenticated(self):
        self.client.credentials()

        url = reverse("archive-approve", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(self.archive.status, ArchiveStatus.WAITING_APPROVAL)

    def test_approve_without_permission(self):
        url = reverse("archive-approve", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(self.archive.status, ArchiveStatus.WAITING_APPROVAL)

    @patch("oais_platform.oais.tasks.process.delay")
    def test_approve_with_permission(self, process_delay):
        self.user.user_permissions.add(self.approve_permission)
        self.user.save()

        url = reverse("archive-approve", args=[self.archive.id])
        response = self.client.post(url, format="json")

        self.archive.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.status, ArchiveStatus.PENDING)
        process_delay.assert_called_once_with(self.archive.id)

    def test_reject_not_waiting_approval(self):
        self.user.user_permissions.add(self.reject_permission)
        self.user.save()

        url = reverse("archive-reject", args=[self.archive.id])
        for archive_status in ArchiveStatus.values:
            if archive_status == ArchiveStatus.WAITING_APPROVAL:
                continue
            self.archive.status = archive_status
            self.archive.save()

            response = self.client.post(url, format="json")

            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
            self.assertEqual(
                response.data["detail"], "Archive is not waiting for approval")

            self.archive.refresh_from_db()
            self.assertEqual(self.archive.status, archive_status)
