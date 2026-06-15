from datetime import timedelta

from django.contrib.auth.models import Permission, User
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    Archive,
    Collection,
    Status,
    Step,
    StepFailureType,
    StepName,
)

COMPLETED = str(Status.COMPLETED.value)
FAILED = str(Status.FAILED.value)


class CollectionSummaryTest(APITestCase):

    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.creator = User.objects.create_user("creator", password="pw")
        self.collection = Collection.objects.create(
            title="test", internal=False, creator=self.creator
        )

    def add_archive_with_steps(self, steps):
        archive = Archive.objects.create(requester=self.creator, approver=self.creator)
        for step in steps:
            Step.objects.create(archive=archive, **step)
        self.collection.add_archive(archive)
        return archive

    def get_summary(
        self, user, type, collection_id=None, permission=None, return_response=False
    ):
        if permission:
            user.user_permissions.set(permission)
        self.client.force_authenticate(user=user)
        url = reverse(
            "tags-summary",
            args=[collection_id or self.collection.id],
        )
        response = self.client.get(
            url,
            {"type": type},
            format="json",
        )
        if return_response:
            return response
        return response.data.get("summary", {})

    def test_summary_groups_latest_step_by_status(self):
        self.add_archive_with_steps(
            [
                {"step_name": StepName.HARVEST, "status": Status.COMPLETED},
                {"step_name": StepName.ARCHIVE, "status": Status.COMPLETED},
            ]
        )
        self.add_archive_with_steps(
            [
                {"step_name": StepName.HARVEST, "status": Status.COMPLETED},
                {"step_name": StepName.ARCHIVE, "status": Status.FAILED},
            ]
        )

        summary = self.get_summary(self.creator, "step")

        self.assertEqual(summary["HARVEST"][COMPLETED]["count"], 2)
        self.assertEqual(summary["ARCHIVE"][COMPLETED]["count"], 1)
        self.assertEqual(summary["ARCHIVE"][FAILED]["count"], 1)

    def test_summary_includes_avg_duration(self):
        self.add_archive_with_steps(
            [{"step_name": StepName.HARVEST, "status": Status.COMPLETED}]
        )

        entry = self.get_summary(self.creator, "step")["HARVEST"][COMPLETED]

        self.assertIn("avg_duration", entry)
        self.assertIsInstance(entry["avg_duration"], float)

    def test_summary_counts_only_latest_attempt_after_retry(self):
        self.add_archive_with_steps(
            [
                {"step_name": StepName.ARCHIVE, "status": Status.FAILED},
                {"step_name": StepName.ARCHIVE, "status": Status.COMPLETED},
            ]
        )

        summary = self.get_summary(self.creator, "step")

        self.assertEqual(summary["ARCHIVE"][COMPLETED]["count"], 1)
        self.assertNotIn(FAILED, summary["ARCHIVE"])

    def test_summary_is_scoped_to_the_collection(self):
        self.add_archive_with_steps(
            [{"step_name": StepName.HARVEST, "status": Status.COMPLETED}]
        )
        other_archive = Archive.objects.create()
        Step.objects.create(
            archive=other_archive,
            step_name=StepName.HARVEST,
            status=Status.FAILED,
        )

        summary = self.get_summary(self.creator, "step")

        self.assertEqual(summary["HARVEST"][COMPLETED]["count"], 1)
        self.assertNotIn(FAILED, summary["HARVEST"])

    def test_summary_is_empty_without_archives(self):
        self.assertEqual(self.get_summary(self.creator, "step"), {})

    def test_failure_summary_groups_by_failure_type(self):
        self.add_archive_with_steps(
            [
                {
                    "step_name": StepName.ARCHIVE,
                    "status": Status.FAILED,
                    "failure_type": StepFailureType.TIMEOUT,
                }
            ]
        )

        failure_summary = self.get_summary(self.creator, "failure")

        self.assertEqual(
            failure_summary["ARCHIVE"], [{"failure_type": "TIMEOUT", "count": 1}]
        )

    def test_failure_summary_labels_missing_failure_type_as_unknown(self):
        self.add_archive_with_steps(
            [{"step_name": StepName.ARCHIVE, "status": Status.FAILED}]
        )

        failure_summary = self.get_summary(self.creator, "failure")

        self.assertEqual(
            failure_summary["ARCHIVE"], [{"failure_type": "Unknown", "count": 1}]
        )

    def test_failure_summary_is_scoped_to_the_collection(self):
        self.add_archive_with_steps(
            [
                {
                    "step_name": StepName.ARCHIVE,
                    "status": Status.FAILED,
                    "failure_type": StepFailureType.TIMEOUT,
                }
            ]
        )
        other_archive = Archive.objects.create()
        Step.objects.create(
            archive=other_archive,
            step_name=StepName.HARVEST,
            status=Status.FAILED,
            failure_type=StepFailureType.HTTP_404,
        )

        failure_summary = self.get_failure_summary()

        self.assertEqual(
            failure_summary["ARCHIVE"], [{"failure_type": "TIMEOUT", "count": 1}]
        )
        self.assertNotIn("HARVEST", failure_summary)

    def test_failure_summary_excludes_steps_whose_latest_attempt_succeeded(self):
        self.add_archive_with_steps(
            [
                {
                    "step_name": StepName.ARCHIVE,
                    "status": Status.FAILED,
                    "failure_type": StepFailureType.TIMEOUT,
                },
                {"step_name": StepName.ARCHIVE, "status": Status.COMPLETED},
            ]
        )

        self.assertEqual(self.get_summary(self.creator, "failure"), {})

    def test_execution_summary_groups_by_step_name(self):
        self.add_archive_with_steps(
            [
                {
                    "step_name": StepName.ARCHIVE,
                    "status": Status.COMPLETED,
                    "start_date": timezone.now(),
                    "finish_date": timezone.now(),
                },
                {
                    "step_name": StepName.ARCHIVE,
                    "status": Status.COMPLETED,
                    "start_date": timezone.now() - timedelta(days=5),
                    "finish_date": timezone.now() - timedelta(days=5),
                },
                {
                    "step_name": StepName.PUSH_TO_CTA,
                    "status": Status.COMPLETED,
                    "start_date": timezone.now(),
                    "finish_date": timezone.now(),
                },
                {
                    "step_name": StepName.HARVEST,
                    "status": Status.COMPLETED,
                    "start_date": timezone.now(),
                    "finish_date": timezone.now(),
                },
            ]
        )

        execution_summary = self.get_summary(self.creator, "execution")

        self.assertEqual(len(execution_summary["PUSH_TO_CTA"]), 1)
        self.assertEqual(len(execution_summary["ARCHIVE"]), 2)
        self.assertRaises(KeyError, lambda: execution_summary["HARVEST"])

    def test_summary_requires_view_permission(self):
        self.add_archive_with_steps(
            [{"step_name": StepName.HARVEST, "status": Status.COMPLETED}]
        )
        response = self.get_summary(self.user, "step", return_response=True)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        permission = Permission.objects.get(codename="view_archive_all")
        summary = self.get_summary(self.user, "step", permission=[permission])
        self.assertIsInstance(summary, dict)

    def test_summary_requires_authentication(self):
        self.add_archive_with_steps(
            [{"step_name": StepName.HARVEST, "status": Status.COMPLETED}]
        )
        response = self.get_summary(self.user, "step", return_response=True)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_summary_invalid_type_returns_bad_request(self):
        self.add_archive_with_steps(
            [{"step_name": StepName.HARVEST, "status": Status.COMPLETED}]
        )
        response = self.get_summary(self.creator, "invalid_type", return_response=True)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_summary_nonexistent_collection_returns_not_found(self):
        response = self.get_summary(
            self.creator, "step", collection_id=999, return_response=True
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
