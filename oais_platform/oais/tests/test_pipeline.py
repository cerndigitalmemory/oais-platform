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
    Steps,
)
from oais_platform.oais.serializers import ArchiveSerializer
from oais_platform.settings import PIPELINE_SIZE_LIMIT


class PipelineTests(APITestCase):
    def setUp(self):
        self.permission = Permission.objects.get(codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")
        self.source = Source.objects.create(
            name="test",
            longname="Test",
            api_url="test.test/api",
            classname="Local",
            notification_enabled=True,
            notification_endpoint="test.test/api/notify",
        )
        self.creator_api_key = ApiKey.objects.create(
            user=self.creator, source=self.source, key="abcd1234"
        )
        self.other_user = User.objects.create_user("other", password="pw")

        self.archive = Archive.objects.create(
            recid="1",
            source=self.source.name,
            source_url="",
            creator=self.creator,
            title="",
            state=ArchiveState.SIP,
        )

        self.dict_archive = ArchiveSerializer(self.archive, many=False)

        self.harvest_step = Step.objects.create(
            archive=self.archive, name=Steps.HARVEST
        )

        self.checksum_step = Step.objects.create(
            archive=self.archive, name=Steps.CHECKSUM
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
            (
                [Steps.PUSH_TO_CTA, Steps.VALIDATION, Steps.CHECKSUM],
                status.HTTP_400_BAD_REQUEST,
            ),  # invalid order
            (
                [Steps.VALIDATION, Steps.CHECKSUM, Steps.HARVEST],
                status.HTTP_400_BAD_REQUEST,
            ),  # invalid order
            (
                [Steps.HARVEST, Steps.VALIDATION, Steps.CHECKSUM, Steps.NOTIFY_SOURCE],
                status.HTTP_400_BAD_REQUEST,
            ),  # invalid order
        ]
    )
    def test_execute_pipeline_invalid_input(self, pipeline, status_code):
        self.client.force_authenticate(user=self.creator)

        url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "pipeline_steps": pipeline},
            format="json",
        )

        self.assertEqual(response.status_code, status_code)
        self.assertEqual(Step.objects.count(), self.init_step_count)

    def test_execute_pipeline_ongoing_execution(self):
        self.client.force_authenticate(user=self.creator)

        pipeline = [Steps.ARCHIVE, Steps.PUSH_TO_CTA, Steps.INVENIO_RDM_PUSH]

        # simulate ongoing checksum step: last_completed_step != last_step
        self.archive.set_last_step(self.checksum_step.id)

        url = reverse("archives-pipeline", kwargs={"pk": self.archive.id})
        response = self.client.post(
            url,
            {"archive": self.dict_archive.data, "pipeline_steps": pipeline},
            format="json",
        )

        archive = Archive.objects.get(pk=self.archive.id)

        self.assertEqual(len(archive.pipeline_steps), len(pipeline))
        self.assertEqual(Step.objects.count(), self.init_step_count + len(pipeline))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @parameterized.expand(
        [
            (
                {
                    "task": "process",
                    "pipeline": [Steps.HARVEST],
                    "prev_step": None,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "task": "validate",
                    "pipeline": [Steps.VALIDATION],
                    "prev_step": Steps.HARVEST,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "task": "checksum",
                    "pipeline": [Steps.CHECKSUM],
                    "prev_step": Steps.VALIDATION,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "task": "archivematica",
                    "pipeline": [Steps.ARCHIVE],
                    "prev_step": Steps.CHECKSUM,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "task": "push_to_cta",
                    "pipeline": [Steps.PUSH_TO_CTA],
                    "prev_step": Steps.ARCHIVE,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "task": "invenio",
                    "pipeline": [Steps.INVENIO_RDM_PUSH],
                    "prev_step": Steps.CHECKSUM,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "task": "extract_title",
                    "pipeline": [Steps.EXTRACT_TITLE],
                    "prev_step": Steps.CHECKSUM,
                },
                status.HTTP_200_OK,
            ),
            (
                {
                    "task": "notify_source",
                    "pipeline": [Steps.NOTIFY_SOURCE],
                    "prev_step": Steps.ARCHIVE,
                },
                status.HTTP_200_OK,
            ),
        ]
    )
    def test_execute_pipeline_one_step(self, input, status_code):

        with patch(f'oais_platform.oais.tasks.{input["task"]}.delay') as task:
            self.client.force_authenticate(user=self.creator)

            step_count = 0

            # set last executed step
            if input["prev_step"]:
                step = Step.objects.create(
                    archive=self.archive,
                    name=input["prev_step"],
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
            match latest_step.name:
                case Steps.HARVEST:
                    task.assert_called_once_with(
                        self.archive.id,
                        latest_step.id,
                        self.creator_api_key.key,
                        input_data=latest_step.output_data,
                    )
                case Steps.EXTRACT_TITLE:
                    task.assert_called_once_with(self.archive.id, latest_step.id)
                case Steps.NOTIFY_SOURCE:
                    task.assert_called_once_with(
                        self.archive.id, latest_step.id, self.creator_api_key.key
                    )
                case _:
                    task.assert_called_once_with(
                        self.archive.id, latest_step.id, latest_step.output_data
                    )

    def test_edit_manifests(self):
        self.client.force_authenticate(user=self.creator)

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
