import json
from unittest.mock import MagicMock, patch

from django.contrib.auth.models import User
from django.test import TestCase
from django.urls import reverse
from django_celery_beat.models import IntervalSchedule, PeriodicTask
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.tasks.archivematica import callback_package
from oais_platform.settings import AM_CALLBACK_DELAY


class AmCallbackViewTest(APITestCase):
    def setUp(self):
        self.superuser = User.objects.create_superuser(
            username="admin", email="admin@test.com", password="testpass"
        )
        self.regular_user = User.objects.create_user(
            username="user", email="user@test.com", password="testpass"
        )
        self.url = reverse("am_callback")  # Adjust URL path as needed

    def test_callback_without_authentication(self):
        """Test callback fails without authentication"""
        data = {"package_uuid": "test-uuid-123", "package_name": "test-package"}

        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_callback_with_regular_user(self):
        """Test callback fails with regular user (not superuser)"""
        self.client.force_authenticate(user=self.regular_user)

        data = {"package_uuid": "test-uuid-123", "package_name": "test-package"}

        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_successful_callback_with_superuser(self):
        """Test successful callback with valid data and superuser permissions"""
        self.client.force_authenticate(user=self.superuser)

        data = {"package_uuid": "test-uuid-123", "package_name": "test-package"}

        with patch("oais_platform.oais.views.callback_package") as mock_task:
            mock_task.delay = MagicMock()

            response = self.client.post(self.url, data, format="json")

            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(response.data, "Callback received.")
            mock_task.delay.assert_called_once_with("test-package")

    def test_callback_missing_package_name(self):
        """Test callback fails when package_name is missing"""
        self.client.force_authenticate(user=self.superuser)

        data = {
            "package_uuid": "test-uuid-123"
            # package_name is missing
        }

        response = self.client.post(self.url, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("package_name is missing", str(response.data))

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

        self.periodic_task = PeriodicTask.objects.create(
            name="test-package-name-task",
            task="check_am_status",
            enabled=True,
            args=json.dumps(["arg1", "arg2"]),
            interval=self.schedule,
        )

        self.another_task = PeriodicTask.objects.create(
            name="another-task",
            task="check_am_status",
            enabled=True,
            args=json.dumps(["other_arg"]),
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
                callback_package("test-package-name")

                self.assertIn(
                    "Callback for package test-package-name received", log.output[0]
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
            ) as log:
                callback_package("nonexistent-package")

                # Verify error was logged
                self.assertIn(
                    "Package with name nonexistent-package not found", log.output[0]
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
            name="test-package-name-another-task",
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
                callback_package("test-package-name")

                # Verify error was logged with count
                self.assertIn(
                    "Ambiguous package name (test-package-name) found: 2", log.output[0]
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

            callback_package("test-package-name")

            # Verify custom delay was used
            mock_check.apply_async.assert_called_once_with(
                args=["arg1", "arg2"], countdown=600
            )
