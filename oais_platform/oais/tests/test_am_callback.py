import json
from unittest.mock import MagicMock, patch

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.archivematica import callback_package, get_task_name
from oais_platform.settings import AM_CALLBACK_DELAY


class AmCallbackViewTest(APITestCase):
    def setUp(self):
        self.superuser = User.objects.create_superuser(
            username="admin", email="admin@test.com", password="testpass"
        )
        self.regular_user = User.objects.create_user(
            username="user", email="user@test.com", password="testpass"
        )
        self.url = reverse("am_callback")
        self.package_name = "cds_abc_Archive_66_Step_12"

    def test_callback_without_authentication(self):
        """Test callback fails without authentication"""
        data = {"package_uuid": "test-uuid-123", "package_name": self.package_name}

        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_callback_with_regular_user(self):
        """Test callback fails with regular user (not superuser)"""
        self.client.force_authenticate(user=self.regular_user)

        data = {"package_uuid": "test-uuid-123", "package_name": self.package_name}

        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_successful_callback_with_superuser(self):
        """Test successful callback with valid data and superuser permissions"""
        self.client.force_authenticate(user=self.superuser)

        data = {"package_uuid": "test-uuid-123", "package_name": self.package_name}

        with patch("oais_platform.oais.views.callback_package") as mock_task:
            mock_task.delay = MagicMock()

            response = self.client.post(self.url, data, format="json")

            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(response.data, "Callback received.")
            mock_task.delay.assert_called_once_with(self.package_name, "test-uuid-123")

    def test_callback_missing_package_name(self):
        """Test callback fails when package_name is missing"""
        self.client.force_authenticate(user=self.superuser)

        data = {
            "package_uuid": "test-uuid-123"
            # package_name is missing
        }

        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("package_name", response.data)
        self.assertEqual(response.data["package_name"][0], "This field is required.")

    def test_callback_empty_package_name(self):
        """Test callback fails when package_name is empty"""
        self.client.force_authenticate(user=self.superuser)

        data = {"package_uuid": "test-uuid-123", "package_name": ""}

        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_callback_none_package_name(self):
        """Test callback fails when package_name is None"""
        self.client.force_authenticate(user=self.superuser)

        data = {"package_uuid": "test-uuid-123", "package_name": None}

        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_callback_with_get_method(self):
        """Test that GET method is not allowed"""
        self.client.force_authenticate(user=self.superuser)

        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)


class AmCallbackTest(TestCase):
    def setUp(self):
        # Create test periodic tasks
        self.schedule, _ = IntervalSchedule.objects.get_or_create(
            every=1, period=IntervalSchedule.HOURS
        )
        self.step = Step.objects.create(
            archive=Archive.objects.create(
                recid="1",
                source="test",
                source_url="",
                path_to_sip="basepath/sips/test_path",
            ),
            step_name=StepName.ARCHIVE,
        )
        self.package_name = f"test_1_Archive_{self.step.archive.id}_Step_{self.step.id}"
        self.package_uuid = "123e4567"

        self.periodic_task = PeriodicTask.objects.create(
            name=get_task_name(self.step),
            enabled=True,
            args=json.dumps(["arg1", "arg2"]),
            interval=self.schedule,
        )

    def test_successful_callback_single_match(self):
        """Test successful callback with single matching periodic task"""
        with patch(
            "oais_platform.oais.tasks.archivematica.check_am_status"
        ) as mock_check:
            mock_check.apply_async = MagicMock()

            with self.assertLogs(
                "oais_platform.oais.tasks.archivematica", level="INFO"
            ) as log:
                callback_package(self.package_name, self.package_uuid)

                self.assertIn(
                    f"Callback for package {self.package_name} received",
                    log.output[0],
                )

                # Verify periodic task was disabled
                self.periodic_task.refresh_from_db()
                self.assertFalse(self.periodic_task.enabled)

                # Verify check_am_status was called with correct args and delay
                mock_check.apply_async.assert_called_once_with(
                    args=["arg1", "arg2"], countdown=AM_CALLBACK_DELAY
                )

    def test_successful_callback_single_match_underscore(self):
        """Test successful callback with single matching periodic task, package name with underscore suffix"""
        with patch(
            "oais_platform.oais.tasks.archivematica.check_am_status"
        ) as mock_check:
            mock_check.apply_async = MagicMock()

            with self.assertLogs(
                "oais_platform.oais.tasks.archivematica", level="INFO"
            ) as log:
                callback_package(
                    self.package_name + "_16", self.package_uuid
                )  # Simulate Archivematica appending a suffix

                self.assertIn(
                    f"Callback for package {self.package_name}_16 received",
                    log.output[0],
                )

                # Verify periodic task was disabled
                self.periodic_task.refresh_from_db()
                self.assertFalse(self.periodic_task.enabled)

                # Verify check_am_status was called with correct args and delay
                mock_check.apply_async.assert_called_once_with(
                    args=["arg1", "arg2"], countdown=AM_CALLBACK_DELAY
                )

    def test_callback_no_matching_task(self):
        """Test callback when no periodic task matches package name"""
        with patch(
            "oais_platform.oais.tasks.archivematica.check_am_status"
        ) as mock_check:
            mock_check.apply_async = MagicMock()

            with self.assertLogs(
                "oais_platform.oais.tasks.archivematica", level="ERROR"
            ) as log_error:
                callback_package("nonexistent-package", "456e7890")

                # Verify error was logged
                self.assertIn(
                    "Could not find step for package nonexistent-package",
                    log_error.output[0],
                )

                # Verify no tasks were called
                mock_check.apply_async.assert_not_called()

                # Verify original task is still enabled
                self.periodic_task.refresh_from_db()
                self.assertTrue(self.periodic_task.enabled)

    def test_callback_multiple_matching_tasks(self):
        """Test callback when multiple periodic tasks match package name"""
        # Create another task with similar name
        duplicate_task = PeriodicTask.objects.create(
            name=f"dup_{self.package_name}",
            task="check_am_status",
            enabled=True,
            args=json.dumps(["dup_arg"]),
            interval=self.schedule,
        )

        with patch(
            "oais_platform.oais.tasks.archivematica.check_am_status"
        ) as mock_check:
            mock_check.apply_async = MagicMock()

            with self.assertLogs(
                "oais_platform.oais.tasks.archivematica", level="ERROR"
            ) as log:
                callback_package(self.package_name, self.package_uuid)

                # Verify error was logged with count
                self.assertIn(
                    f"Ambiguous package name ({self.package_name}) found: 2",
                    log.output[0],
                )

                # Verify no tasks were called
                mock_check.apply_async.assert_not_called()

                # Verify both tasks are still enabled
                self.periodic_task.refresh_from_db()
                duplicate_task.refresh_from_db()
                self.assertTrue(self.periodic_task.enabled)
                self.assertTrue(duplicate_task.enabled)

    @patch("oais_platform.oais.tasks.archivematica.AM_CALLBACK_DELAY", 600)
    def test_callback_with_custom_delay(self):
        """Test callback uses correct delay from settings"""
        with patch(
            "oais_platform.oais.tasks.archivematica.check_am_status"
        ) as mock_check:
            mock_check.apply_async = MagicMock()

            callback_package(self.package_name, self.package_uuid)

            # Verify custom delay was used
            mock_check.apply_async.assert_called_once_with(
                args=["arg1", "arg2"], countdown=600
            )

    def test_callback_already_completed(self):
        """Test callback when task is already marked as completed"""
        self.periodic_task.delete()
        self.step.set_status(Status.COMPLETED)

        with patch(
            "oais_platform.oais.tasks.archivematica.check_am_status"
        ) as mock_check:
            mock_check.apply_async = MagicMock()

            with self.assertLogs(
                "oais_platform.oais.tasks.archivematica", level="INFO"
            ) as log:
                callback_package(self.package_name, self.package_uuid)

                # Verify info log about already processed package
                self.assertIn(
                    f"package {self.package_name} already processed, ignoring callback.",
                    log.output[2],
                )

                # Verify no tasks were called
                mock_check.apply_async.assert_not_called()

    def test_callback_already_failed(self):
        """Test callback when task is already marked as failed"""
        self.periodic_task.delete()
        self.step.set_status(Status.FAILED)

        with patch(
            "oais_platform.oais.tasks.archivematica.check_am_status"
        ) as mock_check:
            mock_check.apply_async = MagicMock()

            with self.assertLogs(
                "oais_platform.oais.tasks.archivematica", level="INFO"
            ) as log:
                callback_package(self.package_name, self.package_uuid)

                # Verify info log about already failed package
                self.assertIn(
                    f"package {self.package_name} already set to {Status.FAILED}, processing callback - recreating Periodic Task.",
                    log.output[2],
                )

                periodic_task = PeriodicTask.objects.latest("id")
                self.assertEqual(periodic_task.name, get_task_name(self.step))
                self.assertEqual(periodic_task.task, "check_am_status")
                self.assertFalse(periodic_task.enabled)
                self.assertEqual(
                    json.loads(periodic_task.args),
                    [self.package_uuid, self.step.id, self.step.archive.id, True],
                )

                # Verify check_am_status was called to recreate periodic task
                mock_check.apply_async.assert_called_once_with(
                    args=[self.package_uuid, self.step.id, self.step.archive.id, True],
                    countdown=AM_CALLBACK_DELAY,
                )

    def test_callback_step_not_found(self):
        """Test callback when step is not found"""
        self.periodic_task.delete()
        self.step.delete()

        with patch(
            "oais_platform.oais.tasks.archivematica.check_am_status"
        ) as mock_check:
            mock_check.apply_async = MagicMock()

            with self.assertLogs(
                "oais_platform.oais.tasks.archivematica", level="WARNING"
            ) as log:
                callback_package(self.package_name, self.package_uuid)

                # Verify error log about step not found
                self.assertIn(
                    f"Could not find step for package {self.package_name}",
                    log.output[1],
                )

                # Verify no tasks were called
                mock_check.apply_async.assert_not_called()
