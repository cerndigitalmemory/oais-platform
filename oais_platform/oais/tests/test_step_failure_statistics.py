from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step, StepFailureType, StepName


class StepTypeStatisticsTestMixin:
    url_name = None
    counted_status = None
    excluded_status = None

    def create_archive_with_steps(self, steps):
        archive = Archive.objects.create()
        for step in steps:
            step_name, step_status = step[0], step[1]
            failure_type = step[2] if len(step) > 2 else None
            Step.objects.create(
                step_name=step_name,
                status=step_status,
                failure_type=failure_type,
                archive=archive,
            )
        archive.save()
        return archive

    def setUp(self):
        self.url = reverse(self.url_name)

    def get_count(self, data, step, failure_type):
        return next(
            (
                row["count"]
                for row in data
                if row["step"] == step and row["failure_type"] == failure_type
            ),
            0,
        )

    def test_step_failure_statistics(self):
        self.create_archive_with_steps(
            [
                (StepName.HARVEST, Status.COMPLETED),
                (StepName.ARCHIVE, self.counted_status, StepFailureType.TIMEOUT),
            ]
        )
        self.create_archive_with_steps(
            [
                (StepName.ARCHIVE, self.counted_status, StepFailureType.TIMEOUT),
            ]
        )
        self.create_archive_with_steps(
            [
                (StepName.HARVEST, self.counted_status, StepFailureType.HTTP_404),
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(
            self.get_count(response.data, StepName.ARCHIVE, StepFailureType.TIMEOUT), 2
        )
        self.assertEqual(
            self.get_count(response.data, StepName.HARVEST, StepFailureType.HTTP_404), 1
        )

    def test_step_failure_statistics_empty_database(self):
        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

    def test_only_failed_steps_are_counted(self):
        self.create_archive_with_steps(
            [
                (StepName.HARVEST, Status.COMPLETED),
                (StepName.ARCHIVE, Status.IN_PROGRESS),
                (StepName.VALIDATION, self.excluded_status, StepFailureType.TIMEOUT),
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

    def test_only_non_zero_combinations_returned(self):
        self.create_archive_with_steps(
            [
                (StepName.ARCHIVE, self.counted_status, StepFailureType.TIMEOUT),
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            self.get_count(response.data, StepName.HARVEST, StepFailureType.TIMEOUT), 0
        )

    def test_retried_step_not_counted_when_latest_attempt_succeeds(self):
        self.create_archive_with_steps(
            [
                (StepName.ARCHIVE, self.counted_status, StepFailureType.TIMEOUT),
                (StepName.ARCHIVE, Status.COMPLETED),
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

    def test_failed_step_without_failure_type(self):
        self.create_archive_with_steps(
            [
                (StepName.ARCHIVE, self.counted_status, StepFailureType.OTHER),
            ]
        )
        self.create_archive_with_steps(
            [
                (StepName.ARCHIVE, self.counted_status, None),
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(any(row["failure_type"] is None for row in response.data))
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            self.get_count(response.data, StepName.ARCHIVE, StepFailureType.OTHER), 2
        )

    def test_retried_step_counted_when_latest_attempt_fails(self):
        self.create_archive_with_steps(
            [
                (StepName.ARCHIVE, self.counted_status, StepFailureType.TIMEOUT),
                (
                    StepName.ARCHIVE,
                    self.counted_status,
                    StepFailureType.CONNECTION_ERROR,
                ),
            ]
        )

        response = self.client.get(self.url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(
            self.get_count(response.data, StepName.ARCHIVE, StepFailureType.TIMEOUT), 0
        )
        self.assertEqual(
            self.get_count(
                response.data, StepName.ARCHIVE, StepFailureType.CONNECTION_ERROR
            ),
            1,
        )


class StepFailureStatisticsEndpointTest(StepTypeStatisticsTestMixin, APITestCase):
    url_name = "step_failure_statistics"
    counted_status = Status.FAILED
    excluded_status = Status.COMPLETED_WITH_WARNINGS


class StepWarningStatisticsEndpointTest(StepTypeStatisticsTestMixin, APITestCase):
    url_name = "step_warning_statistics"
    counted_status = Status.COMPLETED_WITH_WARNINGS
    excluded_status = Status.FAILED
