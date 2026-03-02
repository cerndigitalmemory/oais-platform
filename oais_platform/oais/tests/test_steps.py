from django.contrib.auth.models import Permission, User
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

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

        self.removable_step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.PUSH_TO_CTA,
            status=Status.WAITING,
        )
        self.archive.pipeline_steps = [self.removable_step.id]
        self.archive.save()

    def test_delete_step_unauthenticated(self):
        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.removable_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_delete_step_permissions_no_view(self):
        self.client.force_authenticate(user=self.testuser)

        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.removable_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_delete_step_permissions_no_execute(self):
        self.testuser.user_permissions.add(self.permission)
        self.client.force_authenticate(user=self.testuser)

        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.removable_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_delete_step_permissions_execute(self):
        self.testuser.user_permissions.add(self.permission)
        self.testuser.user_permissions.add(self.execute_permission)
        self.client.force_authenticate(user=self.testuser)

        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.removable_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertFalse(Step.objects.filter(id=self.removable_step.id).exists())
        self.archive.refresh_from_db()
        self.assertNotIn(self.removable_step.id, self.archive.pipeline_steps)

    def test_delete_step_superuser(self):
        self.client.force_authenticate(user=self.superuser)

        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.removable_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(Step.objects.filter(id=self.removable_step.id).exists())

        self.archive.refresh_from_db()
        self.assertNotIn(self.removable_step.id, self.archive.pipeline_steps)

    def test_delete_step_owner_without_permissions(self):
        self.client.force_authenticate(user=self.owner)

        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.removable_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_delete_step_owner_with_permissions(self):
        self.owner.user_permissions.add(self.execute_permission)
        self.client.force_authenticate(user=self.owner)

        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.removable_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(Step.objects.filter(id=self.removable_step.id).exists())

        self.archive.refresh_from_db()
        self.assertNotIn(self.removable_step.id, self.archive.pipeline_steps)

    def test_delete_step_not_removable(self):
        self.client.force_authenticate(user=self.superuser)

        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.harvest_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertTrue(Step.objects.filter(id=self.harvest_step.id).exists())
        self.archive.refresh_from_db()
        self.assertIn(self.removable_step.id, self.archive.pipeline_steps)

    def test_delete_step_with_celery_task_id(self):
        self.client.force_authenticate(user=self.superuser)
        self.removable_step.celery_task_id = "test-task-id"
        self.removable_step.save()

        response = self.client.delete(
            reverse("steps-delete", kwargs={"pk": self.removable_step.id})
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertTrue(Step.objects.filter(id=self.removable_step.id).exists())
        self.archive.refresh_from_db()
        self.assertIn(self.removable_step.id, self.archive.pipeline_steps)

    def test_delete_step_not_found(self):
        self.client.force_authenticate(user=self.superuser)

        response = self.client.delete(reverse("steps-delete", kwargs={"pk": 999}))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
