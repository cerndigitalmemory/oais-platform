from unittest.mock import patch

from django.contrib.auth.models import Permission, User
from django.db import IntegrityError
from django.urls import reverse
from parameterized import parameterized
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    Archive,
    ArchiveState,
    Collection,
    Resource,
    Status,
    Step,
    StepName,
    StepType,
)

FILTER_TEST_CASES = [
    (
        {"access": "all", "filters": {"source": "test", "query": "1"}},
        {"status": status.HTTP_200_OK, "size": 2},
    ),
    (
        {"access": "all", "filters": {"query": "723"}},
        {"status": status.HTTP_200_OK, "size": 1},
    ),
    (
        {"access": "all", "filters": {"query": "archive"}},
        {"status": status.HTTP_200_OK, "size": 2},
    ),
    (
        {"access": "all", "filters": {"source": "test2", "query": "1"}},
        {"status": status.HTTP_200_OK, "size": 0},
    ),
    (
        lambda self: {
            "access": "all",
            "filters": {"exclude_tag": str(self.private_tag.id)},
        },
        {"status": status.HTTP_200_OK, "size": 3},
    ),
    ({"access": "all"}, {"status": status.HTTP_400_BAD_REQUEST, "size": 0}),
    (
        {
            "access": "all",
            "filters": {
                "source": "test",
                "step_filters": {
                    "or": [{"name": StepName.HARVEST, "status": Status.COMPLETED}]
                },
            },
        },
        {"status": status.HTTP_200_OK, "size": 2},
    ),
    (
        {
            "access": "all",
            "filters": {
                "step_filters": {
                    "or": [
                        {
                            "and": [
                                {"name": StepName.HARVEST, "status": Status.COMPLETED},
                                {
                                    "name": StepName.VALIDATION,
                                    "status": Status.COMPLETED,
                                    "exclude": True,
                                },
                            ]
                        },
                        {
                            "name": StepName.EXTRACT_TITLE,
                            "status": Status.COMPLETED,
                            "last_step": True,
                        },
                    ]
                }
            },
        },
        {"status": status.HTTP_200_OK, "size": 1},
    ),
    (
        {
            "access": "all",
            "filters": {
                "step_filters": {
                    "or": [
                        {
                            "and": [
                                {"name": StepName.HARVEST},
                                {"name": StepName.VALIDATION},
                                {"name": StepName.EXTRACT_TITLE, "exclude": True},
                            ],
                        },
                        {"status": Status.COMPLETED},
                    ]
                }
            },
        },
        {"status": status.HTTP_400_BAD_REQUEST},
    ),
    (
        {
            "access": "all",
            "filters": {
                "source": "test",
                "step_filters": {"or": [{"invalid_key": StepName.HARVEST}]},
            },
        },
        {"status": status.HTTP_400_BAD_REQUEST},
    ),
]


class ArchiveTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="view_archive_all")
        self.approve_permission = Permission.objects.get(codename="can_approve_all")
        self.superuser = User.objects.create_superuser("superuser", password="pw")
        self.requester = User.objects.create_user("requester", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")

        self.private_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.requester,
            restricted=True,
        )

        self.private_tag = Collection.objects.create(
            internal=False,
            creator=self.requester,
        )
        self.private_tag.add_archive(self.private_archive)

        self.superuser_tag = Collection.objects.create(
            internal=False,
            creator=self.superuser,
        )
        self.superuser_tag.add_archive(self.private_archive)

        self.public_archives = []
        resources = [
            ["1", "test", "test source 1"],
            ["7234", "source_1", "archive test 1"],
            ["3445", "source_2", "archive test 2"],
        ]
        for r in resources:
            archive = Archive.objects.create(
                recid=r[0],
                source=r[1],
                source_url="",
                requester=self.requester,
                restricted=False,
                title=r[2],
            )
            self.public_archives.append(archive)

    def create_steps(self):
        Step.objects.create(
            archive=self.private_archive,
            step_name=StepName.HARVEST,
            status=Status.COMPLETED,
        )
        Step.objects.create(
            archive=self.private_archive,
            step_name=StepName.VALIDATION,
            status=Status.FAILED,
        )
        last_step = Step.objects.create(
            archive=self.private_archive,
            step_name=StepName.EXTRACT_TITLE,
            status=Status.COMPLETED,
        )
        self.private_archive.set_last_step(last_step)

        for p in self.public_archives:
            Step.objects.create(
                archive=p, step_name=StepName.HARVEST, status=Status.COMPLETED
            )
            last_step = Step.objects.create(
                archive=p, step_name=StepName.VALIDATION, status=Status.COMPLETED
            )
            p.set_last_step(last_step)

    def test_archive_list_public(self):
        url = reverse("archives-list")
        response = self.client.get(url, {"access": "public"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), len(self.public_archives))
        for result in response.data["results"]:
            self.assertEqual(
                result["id"]
                in list(map(lambda archive: archive.id, self.public_archives)),
                True,
            )

    @parameterized.expand(
        [
            ({"access": "all"}, {"status": status.HTTP_200_OK, "size": 4}),
            ({"access": "owned"}, {"status": status.HTTP_200_OK, "size": 0}),
        ]
    )
    def test_archive_list_superuser(self, access, output):
        self.client.force_authenticate(user=self.superuser)

        url = reverse("archives-list")
        response = self.client.get(url, access, format="json")

        self.assertEqual(response.status_code, output["status"])
        self.assertEqual(len(response.data["results"]), output["size"])

    @parameterized.expand(
        [
            ({"access": "all"}, {"status": status.HTTP_200_OK, "size": 3}),
            ({"access": "owned"}, {"status": status.HTTP_200_OK, "size": 0}),
        ]
    )
    def test_archive_list_other_user(self, access, output):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, access, format="json")

        self.assertEqual(response.status_code, output["status"])
        self.assertEqual(len(response.data["results"]), output["size"])

    @parameterized.expand(
        [
            ({"access": "all"}, {"status": status.HTTP_200_OK, "size": 4}),
            ({"access": "owned"}, {"status": status.HTTP_200_OK, "size": 4}),
        ]
    )
    def test_archive_list_requester_user(self, access, output):
        self.client.force_authenticate(user=self.requester)

        url = reverse("archives-list")
        response = self.client.get(url, access, format="json")

        self.assertEqual(response.status_code, output["status"])
        self.assertEqual(len(response.data["results"]), output["size"])

    def test_archive_list_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-list")
        response = self.client.get(url, {"access": "all"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 4)

    def _run_filter_test(self, url_name, result_key, data, output):
        if callable(data):
            data = data(self)  # Resolve the lambda function

        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse(url_name)
        response = self.client.post(url, data, format="json")
        self.assertEqual(response.status_code, output["status"])

        if response.status_code == status.HTTP_200_OK:
            self.assertEqual(len(response.data[result_key]), output["size"])

    @parameterized.expand(FILTER_TEST_CASES)
    def test_archives_filtered(self, data, output):
        self.create_steps()
        self._run_filter_test("archives-filter", "results", data, output)

    @parameterized.expand(FILTER_TEST_CASES)
    def test_archives_filtered_ids(self, data, output):
        self.create_steps()
        self._run_filter_test("archives-filter-ids", "ids", data, output)

    def test_archive_details_requester(self):
        self.client.force_authenticate(user=self.requester)

        url = reverse("archives-detail", args=[self.private_archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_archive_details_superuser(self):
        self.client.force_authenticate(user=self.superuser)

        url = reverse("archives-detail", args=[self.private_archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_archive_details_other_user(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-detail", args=[self.private_archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_archive_details_other_user_with_perm(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-detail", args=[self.private_archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.private_archive.id)

    def test_get_steps(self):
        self.client.force_authenticate(user=self.requester)

        url = reverse("archives-steps", args=[self.private_archive.id])
        response = self.client.get(
            url,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        self.step1 = Step.objects.create(
            archive=self.private_archive,
            step_name=StepName.VALIDATION,
            status=Status.COMPLETED,
            start_date="2024-01-01T00:00:00Z",
        )
        self.step2 = Step.objects.create(
            archive=self.private_archive,
            step_name=StepName.ARCHIVE,
            status=Status.FAILED,
            start_date="2024-01-01T00:01:00Z",
        )
        self.step3 = Step.objects.create(
            archive=self.private_archive,
            step_name=StepName.NOTIFY_SOURCE,
            status=Status.WAITING,
        )
        self.private_archive.last_step = self.step2
        self.private_archive.last_completed_step = self.step1
        self.private_archive.pipeline_steps = [self.step3.id]
        self.private_archive.save()

        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 3)
        self.assertEqual(response.data[0]["id"], self.step1.id)
        self.assertEqual(response.data[1]["id"], self.step2.id)
        self.assertEqual(response.data[2]["id"], self.step3.id)

        # Create waiting retry step
        self.step4 = Step.objects.create(
            archive=self.private_archive,
            step_name=StepName.ARCHIVE,
            status=Status.WAITING,
        )
        self.private_archive.last_step = self.step4
        self.private_archive.save()

        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(response.data[0]["id"], self.step1.id)
        self.assertEqual(response.data[1]["id"], self.step2.id)
        self.assertEqual(response.data[2]["id"], self.step4.id)
        self.assertEqual(response.data[3]["id"], self.step3.id)

    def test_record_check_none(self):
        self.client.force_authenticate(user=self.requester)

        url = reverse("archives-duplicates")
        response = self.client.post(
            url, {"records": [{"recid": "1", "source": "test"}]}, format="json"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]["duplicates"]), 0)

    def test_record_check(self):
        self.client.force_authenticate(user=self.requester)
        Step.objects.create(
            archive=self.private_archive, step_name=StepName.HARVEST, status=4
        )
        Step.objects.create(
            archive=self.private_archive, step_name=StepName.ARCHIVE, status=4
        )
        Step.objects.create(
            archive=self.public_archives[0], step_name=StepName.HARVEST, status=4
        )
        Step.objects.create(
            archive=self.public_archives[0], step_name=StepName.ARCHIVE, status=4
        )

        url = reverse("archives-duplicates")
        response = self.client.post(
            url, {"records": [{"recid": "1", "source": "test"}]}, format="json"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(len(response.data[0]["duplicates"]), 2)
        dup_ids = list(
            Resource.objects.filter(recid="1", source="test").values_list(
                "archive__id", flat=True
            )
        )
        for duplicate in response.data[0]["duplicates"]:
            self.assertIn(duplicate["id"], dup_ids)

    def test_resource_created(self):
        self.assertEqual(Resource.objects.all().count(), 3)
        # This recid already exists. Therefore, the number of objects should not increase
        Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.requester,
            restricted=True,
        )
        self.assertEqual(Resource.objects.all().count(), 3)
        Archive.objects.create(
            recid="2",
            source="test",
            source_url="",
            requester=self.requester,
            restricted=True,
        )
        self.assertEqual(Resource.objects.all().count(), 4)

        with self.assertRaises(IntegrityError):
            Resource.objects.create(recid="2", source="test")

    def test_get_archives_sources(self):
        self.client.force_authenticate(user=self.requester)

        url = reverse("archives-sources")
        response = self.client.get(
            url,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(list(response.data), ["source_1", "source_2", "test"])

    def test_archive_tags_requester(self):
        self.client.force_authenticate(user=self.requester)

        url = reverse("archives-tags", args=[self.private_archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 1)
        self.assertEqual(response.data["results"][0]["id"], self.private_tag.id)

    def test_archive_tags_other_user(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-tags", args=[self.private_archive.id])
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        self.requester.user_permissions.add(self.permission)
        self.requester.save()

        self.client.force_authenticate(user=self.requester)
        response = self.client.get(url, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data["results"]), 2)

    def test_archive_mlt_unstage_forbidden(self):
        self.client.force_authenticate(user=self.requester)

        url = reverse("archives-mlt-unstage")
        response = self.client.post(
            url, {"archives": [{"id": self.private_archive.id}]}, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_archive_mlt_unstage_with_perms(self, mock_dispatch):
        self.requester.user_permissions.add(self.approve_permission)
        self.requester.save()
        self.client.force_authenticate(user=self.requester)

        other_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.other_user,
            restricted=True,
            staged=True,
        )

        url = reverse("archives-mlt-unstage")
        response = self.client.post(
            url,
            {"archives": [{"id": self.private_archive.id}, {"id": other_archive.id}]},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        response = self.client.post(
            url, {"archives": [{"id": self.private_archive.id}]}, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.private_archive.refresh_from_db()
        mock_dispatch.assert_called_once_with(
            StepType.get_by_stepname(StepName.HARVEST),
            self.private_archive.id,
            self.private_archive.last_step.id,
            False,
        )
        step = Step.objects.last()
        self.assertEqual(step.initiated_by_user, self.requester)
        self.assertEqual(step.initiated_by_harvest_batch, None)
        job = Collection.objects.order_by("-timestamp").first()
        self.assertRegex(job.title, r"^Job \d{4}-\d{2}-\d{2} \d{2}:\d{2}$")
        self.assertEqual(job.creator, self.requester)
        self.assertEqual(job.archives.count(), 1)

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_archive_mlt_unstage_superuser(self, mock_dispatch):
        self.client.force_authenticate(user=self.superuser)

        other_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.other_user,
            restricted=True,
            staged=True,
        )

        url = reverse("archives-mlt-unstage")
        response = self.client.post(
            url,
            {
                "archives": [{"id": self.private_archive.id}, {"id": other_archive.id}],
                "job_title": "Test123",
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(mock_dispatch.call_count, 2)
        self.private_archive.refresh_from_db()
        other_archive.refresh_from_db()
        self.assertEqual(
            mock_dispatch.mock_calls[0].args,
            (
                StepType.get_by_stepname(StepName.HARVEST),
                self.private_archive.id,
                self.private_archive.last_step.id,
                False,
            ),
        )
        self.assertEqual(
            mock_dispatch.mock_calls[1].args,
            (
                StepType.get_by_stepname(StepName.HARVEST),
                other_archive.id,
                other_archive.last_step.id,
                False,
            ),
        )
        step = Step.objects.last()
        self.assertEqual(step.initiated_by_user, self.superuser)
        self.assertEqual(step.initiated_by_harvest_batch, None)
        job = Collection.objects.order_by("-timestamp").first()
        self.assertEqual(job.title, "Test123")
        self.assertEqual(job.creator, self.superuser)
        self.assertEqual(job.archives.count(), 2)

    def test_archive_delete_staged_other_user(self):
        self.client.force_authenticate(user=self.other_user)

        url = reverse("archives-delete-staged", args=[self.private_archive.id])
        response = self.client.post(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_archive_delete_staged_other_user_with_perms(self):
        self.private_archive.staged = False
        self.private_archive.save()
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()

        self.client.force_authenticate(user=self.other_user)
        url = reverse("archives-delete-staged", args=[self.private_archive.id])
        response = self.client.post(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        self.private_archive.staged = True
        self.private_archive.save()
        response = self.client.post(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.filter(id=self.private_archive.id).count(), 0)

    def test_archive_delete_staged_superuser(self):
        self.private_archive.staged = False
        self.private_archive.save()
        self.client.force_authenticate(user=self.superuser)

        url = reverse("archives-delete-staged", args=[self.private_archive.id])
        response = self.client.post(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        self.private_archive.staged = True
        self.private_archive.save()

        response = self.client.post(url, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(Archive.objects.filter(id=self.private_archive.id).count(), 0)

    def test_archive_actions_forbidden(self):
        self.client.force_authenticate(user=self.requester)

        other_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.other_user,
            restricted=True,
            staged=True,
        )

        url = reverse("archives-actions")
        response = self.client.post(
            url,
            {
                "archives": [
                    {"id": self.private_archive.id, "state": ArchiveState.NONE},
                    {"id": other_archive.id, "state": ArchiveState.NONE},
                ]
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_archive_actions_with_perms(self):
        self.requester.user_permissions.add(self.permission)
        self.requester.save()
        self.client.force_authenticate(user=self.requester)

        other_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.other_user,
            restricted=True,
            staged=True,
        )

        url = reverse("archives-actions")
        response = self.client.post(
            url,
            {
                "archives": [
                    self.private_archive.id,
                    other_archive.id,
                ]
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["all_last_step_failed"], False)
        self.assertEqual(response.data["can_continue"], False)

    def test_archive_actions_superuser(self):
        self.client.force_authenticate(user=self.superuser)

        other_archive = Archive.objects.create(
            recid="1",
            source="test",
            source_url="",
            requester=self.other_user,
            restricted=True,
            staged=True,
        )

        url = reverse("archives-actions")
        response = self.client.post(
            url,
            {
                "archives": [
                    self.private_archive.id,
                    other_archive.id,
                ]
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["all_last_step_failed"], False)
        self.assertEqual(response.data["can_continue"], False)
