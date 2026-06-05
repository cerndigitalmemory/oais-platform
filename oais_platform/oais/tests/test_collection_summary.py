from django.contrib.auth.models import User
from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    Archive,
    Collection,
    Status,
    Step,
    StepFailureType,
    StepName,
)
from oais_platform.oais.serializers import CollectionSerializer

COMPLETED = str(Status.COMPLETED.value)
FAILED = str(Status.FAILED.value)


class CollectionSummarySerializerTest(APITestCase):

    def setUp(self):
        self.creator = User.objects.create_user("creator", password="pw")
        self.collection = Collection.objects.create(
            title="test", internal=False, creator=self.creator
        )

    def add_archive_with_steps(self, steps):
        archive = Archive.objects.create()
        for step in steps:
            Step.objects.create(archive=archive, **step)
        self.collection.add_archive(archive)
        return archive

    def get_summary(self):
        return CollectionSerializer(self.collection).data["archives_summary"]

    def get_failure_summary(self):
        return CollectionSerializer(self.collection).data["archives_failure_summary"]

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

        summary = self.get_summary()

        self.assertEqual(summary["HARVEST"][COMPLETED]["count"], 2)
        self.assertEqual(summary["ARCHIVE"][COMPLETED]["count"], 1)
        self.assertEqual(summary["ARCHIVE"][FAILED]["count"], 1)

    def test_summary_includes_avg_duration(self):
        self.add_archive_with_steps(
            [{"step_name": StepName.HARVEST, "status": Status.COMPLETED}]
        )

        entry = self.get_summary()["HARVEST"][COMPLETED]

        self.assertIn("avg_duration", entry)
        self.assertIsInstance(entry["avg_duration"], float)

    def test_summary_counts_only_latest_attempt_after_retry(self):
        self.add_archive_with_steps(
            [
                {"step_name": StepName.ARCHIVE, "status": Status.FAILED},
                {"step_name": StepName.ARCHIVE, "status": Status.COMPLETED},
            ]
        )

        summary = self.get_summary()

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

        summary = self.get_summary()

        self.assertEqual(summary["HARVEST"][COMPLETED]["count"], 1)
        self.assertNotIn(FAILED, summary["HARVEST"])

    def test_summary_is_empty_without_archives(self):
        self.assertEqual(self.get_summary(), {})

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

        failure_summary = self.get_failure_summary()

        self.assertEqual(
            failure_summary["ARCHIVE"], [{"failure_type": "TIMEOUT", "count": 1}]
        )

    def test_failure_summary_labels_missing_failure_type_as_unknown(self):
        self.add_archive_with_steps(
            [{"step_name": StepName.ARCHIVE, "status": Status.FAILED}]
        )

        failure_summary = self.get_failure_summary()

        self.assertEqual(
            failure_summary["ARCHIVE"], [{"failure_type": "Unknown", "count": 1}]
        )

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

        self.assertEqual(self.get_failure_summary(), {})
