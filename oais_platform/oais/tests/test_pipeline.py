from unittest.mock import patch

from django.contrib.auth.models import Permission, User
from django.urls import reverse
from parameterized import parameterized
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    ArchiveState,
    Source,
    Status,
    Step,
    StepName,
    StepType,
)
from oais_platform.oais.serializers import ArchiveSerializer
from oais_platform.settings import PIPELINE_SIZE_LIMIT


class PipelineTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="view_archive_all")
        self.execute_permission = Permission.objects.get(codename="can_execute_step")
        self.edit_permission = Permission.objects.get(codename="can_edit_all")

        self.testuser = User.objects.create_user("testuser", password="pw")
        self.testuser.user_permissions.add(self.execute_permission)
        self.testuser.save()

        self.source = Source.objects.create(
            name="test",
            longname="Test",
            api_url="test.test/api",
            classname="Local",
            notification_enabled=True,
            notification_endpoint="test.test/api/notify",
        )
        self.testuser_api_key = ApiKey.objects.create(
            user=self.testuser, source=self.source, key="abcd1234"
        )
        self.other_user = User.objects.create_user("other", password="pw")

        self.archive = Archive.objects.create(
            recid="1",
            source=self.source.name,
            source_url="",
            requester=self.testuser,
            approver=self.testuser,
            title="",
            state=ArchiveState.SIP,
        )

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        self.harvest_step = Step.objects.create(
            archive=self.archive, step_name=StepName.HARVEST
        )

        self.validation_step = Step.objects.create(
            archive=self.archive, step_name=StepName.VALIDATION
        )

        self.init_step_count = Step.objects.count()

        self.archive.set_last_completed_step(self.harvest_step.id)
        self.archive.set_last_step(self.harvest_step.id)

    @parameterized.expand(
        [
            (
                [i for i in range(PIPELINE_SIZE_LIMIT + 1)],
                status.HTTP_400_BAD_REQUEST,
            ),  # invalid size
            ([-1], status.HTTP_400_BAD_REQUEST),  # invalid type of the step
        ]
    )
    def test_execute_pipeline_invalid_input(self, pipeline, status_code):
        self.client.force_authenticate(user=self.testuser)

        url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "pipeline_steps": pipeline},
            format="json",
        )

        self.assertEqual(response.status_code, status_code)
        self.assertEqual(Step.objects.count(), self.init_step_count)

    def test_execute_pipeline_ongoing_execution(self):
        self.client.force_authenticate(user=self.testuser)

        pipeline = [StepName.ARCHIVE, StepName.PUSH_TO_CTA, StepName.INVENIO_RDM_PUSH]

        url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "pipeline_steps": pipeline},
            format="json",
        )

        archive = Archive.objects.get(pk=self.archive.id)

        self.assertEqual(len(archive.pipeline_steps), len(pipeline) - 1)
        self.assertEqual(Step.objects.count(), self.init_step_count + len(pipeline))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_execute_pipeline_forbidden(self):
        self.client.force_authenticate(user=self.other_user)

        pipeline = [StepName.VALIDATION]

        url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "pipeline_steps": pipeline},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_execute_pipeline_with_perms(self, mock_dispatch):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.user_permissions.add(self.execute_permission)
        self.other_user.save()
        self.client.force_authenticate(user=self.other_user)

        pipeline = [StepName.VALIDATION]

        url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "pipeline_steps": pipeline},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.archive.refresh_from_db()
        mock_dispatch.assert_called_once_with(
            StepType.get_by_stepname(StepName.VALIDATION),
            self.archive.id,
            self.archive.last_step.id,
            None,
            None,
            False,
        )

    @parameterized.expand(
        [
            (
                {
                    "pipeline": [StepName.HARVEST],
                    "prev_step": None,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "pipeline": [StepName.VALIDATION],
                    "prev_step": StepName.HARVEST,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "pipeline": [StepName.PUSH_TO_CTA],
                    "prev_step": StepName.ARCHIVE,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "pipeline": [StepName.NOTIFY_SOURCE],
                    "prev_step": StepName.ARCHIVE,
                },
                status.HTTP_200_OK,
            ),
        ]
    )
    def test_execute_pipeline_one_step(self, input, status_code):

        with patch(
            "oais_platform.oais.tasks.pipeline_actions.dispatch_task"
        ) as mock_dispatch:
            self.client.force_authenticate(user=self.testuser)

            step_count = 0

            # set last executed step
            if input["prev_step"]:
                step = Step.objects.create(
                    archive=self.archive,
                    step_name=input["prev_step"],
                    status=Status.COMPLETED,
                )
                self.archive.set_last_completed_step(step.id)
                self.archive.set_last_step(step.id)
                step_count += 1
            else:
                self.archive.last_completed_step = None
                self.archive.last_step = None
                self.archive.save()

            pipeline = input["pipeline"]

            url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
            response = self.client.post(
                url,
                {"archive": self.dict_archive.data, "pipeline_steps": pipeline},
                format="json",
            )

            latest_step = Step.objects.latest("id")

            self.assertEqual(response.status_code, status_code)
            self.assertEqual(
                Step.objects.count(), self.init_step_count + len(pipeline) + step_count
            )
            self.assertEqual(latest_step.status, Status.WAITING)
            self.assertEqual(latest_step.input_step, self.archive.last_completed_step)
            self.assertEqual(
                Archive.objects.get(pk=self.archive.id).last_step.id, latest_step.id
            )
            mock_dispatch.assert_called_once_with(
                StepType.get_by_stepname(input["pipeline"][0]),
                self.archive.id,
                latest_step.id,
                latest_step.input_data,
                self.testuser_api_key.key,
                False,
            )

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_step_disabled(self, mock_dispatch):
        StepType.objects.get(name=StepName.VALIDATION).set_enabled(False)
        self.client.force_authenticate(user=self.testuser)
        url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {
                "archive": self.dict_archive.data,
                "pipeline_steps": [StepName.VALIDATION],
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        latest_step = Step.objects.latest("id")
        self.assertEqual(latest_step.status, Status.FAILED)
        mock_dispatch.assert_not_called()

    def test_edit_manifests(self):
        self.client.force_authenticate(user=self.testuser)

        self.assertEqual(self.archive.manifest, None)

        url = reverse("archives-save-manifest", args=[self.archive.id])
        response = self.client.post(
            url,
            {"manifest": {"test": "test"}},
            format="json",
        )

        self.archive.refresh_from_db()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.manifest, {"test": "test"})

    def test_edit_manifests_forbidden(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.save()
        self.client.force_authenticate(user=self.other_user)

        self.assertEqual(self.archive.manifest, None)

        url = reverse("archives-save-manifest", args=[self.archive.id])
        response = self.client.post(
            url,
            {"manifest": {"test": "test"}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_edit_manifests_with_perms(self):
        self.other_user.user_permissions.add(self.permission)
        self.other_user.user_permissions.add(self.edit_permission)
        self.other_user.save()
        self.client.force_authenticate(user=self.other_user)

        self.assertEqual(self.archive.manifest, None)

        url = reverse("archives-save-manifest", args=[self.archive.id])
        response = self.client.post(
            url,
            {"manifest": {"test": "test"}},
            format="json",
        )

        self.archive.refresh_from_db()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(self.archive.manifest, {"test": "test"})

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_extract_title_success(self, mock_dispatch):
        self.client.force_authenticate(user=self.testuser)
        Step.objects.create(
            archive=self.archive, step_name=StepName.HARVEST, status=Status.COMPLETED
        )

        url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {
                "archive": self.dict_archive.data,
                "pipeline_steps": [StepName.EXTRACT_TITLE],
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        latest_step = Step.objects.latest("id")
        self.assertEqual(latest_step.step_type.name, StepName.EXTRACT_TITLE)
        self.assertEqual(latest_step.status, Status.WAITING)

        mock_dispatch.assert_called_once_with(
            StepType.get_by_stepname(StepName.EXTRACT_TITLE),
            self.archive.id,
            latest_step.id,
            latest_step.input_data,
            self.testuser_api_key.key,
            False,
        )
