import json
import os
import tempfile
from unittest import skip
from unittest.mock import patch

from bagit_create import main as bic
from django.contrib.auth.models import User
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Collection, Steps
from oais_platform.oais.tasks.announce import batch_announce_task


class BatchAnnounceTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("user", "", "pw")
        self.client.force_authenticate(user=self.user)

        self.tag = Collection.objects.create(
            title="celery_test",
            description="Batch Announce processing...",
            creator=self.user,
            internal=False,
        )

    @skip("Only admins can batch announce for now")
    def test_batch_announce_wrong_path(self):
        url = reverse("batch-announce")

        post_data = {
            "batch_announce_path": "/eos/user/s/standarduser/announce",
            "batch_tag": "test_tag",
        }

        response = self.client.post(url, post_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["detail"], "You're not allowed to announce this path"
        )
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Collection.objects.count(), 1)

    def test_batch_announce_duplicate_tag(self):
        url = reverse("batch-announce")

        Collection.objects.create(title="test_tag", internal=False, creator=self.user)

        post_data = {
            "batch_announce_path": "/eos/user/u/user/announce",
            "batch_tag": "test_tag",
        }

        response = self.client.post(url, post_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["detail"], "A tag with the same name already exists!"
        )
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Collection.objects.count(), 2)

    def test_batch_announce_folder_does_not_exist(self):
        url = reverse("batch-announce")

        post_data = {
            "batch_announce_path": "/eos/home-u/user/announce",
            "batch_tag": "test_tag",
        }

        response = self.client.post(url, post_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["detail"],
            "Folder does not exist or the oais user has no access",
        )
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Collection.objects.count(), 1)

    @patch("oais_platform.oais.tasks.announce.batch_announce_task.delay")
    def test_batch_announce(self, batch_announce_delay):
        url = reverse("batch-announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)
            # Test that file does not affect subfolder count
            with open(os.path.join(batch_announce_folder, "empty_file"), "w"):
                pass

            bic.process(
                recid="njf9e-1q233",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            bic.process(
                recid="yz39b-yf220",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            post_data = {
                "batch_announce_path": batch_announce_folder,
                "batch_tag": "test_tag",
            }

            with override_settings(BATCH_ANNOUNCE_LIMIT=2):
                response = self.client.post(url, post_data, format="json")

        tag = Collection.objects.latest("id")
        self.assertRedirects(
            response,
            reverse("tags-detail", kwargs={"pk": tag.id}),
            status_code=302,
        )
        self.assertEqual(
            tag.description,
            "Batch Announce processing...",
        )
        self.assertEqual(tag.title, "test_tag")
        batch_announce_delay.assert_called_once_with(
            batch_announce_folder, tag.id, self.user.id
        )

    @patch("oais_platform.oais.tasks.announce.batch_announce_task.delay")
    def test_batch_announce_limit_exceeded(self, batch_announce_delay):
        url = reverse("batch-announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)

            bic.process(
                recid="1x3p3-e6505",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            bic.process(
                recid="yz39b-yf220",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            post_data = {
                "batch_announce_path": batch_announce_folder,
                "batch_tag": "test_tag",
            }

            with override_settings(BATCH_ANNOUNCE_LIMIT=1):
                response = self.client.post(url, post_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["detail"],
            "Number of subfolder limit exceeded (limit: 1)",
        )
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Collection.objects.count(), 1)
        batch_announce_delay.assert_not_called()

    @patch("oais_platform.oais.tasks.announce.batch_announce_task.delay")
    def test_batch_announce_no_subfolders(self, batch_announce_delay):
        url = reverse("batch-announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)

            post_data = {
                "batch_announce_path": batch_announce_folder,
                "batch_tag": "test_tag",
            }

            with override_settings(BATCH_ANNOUNCE_LIMIT=1):
                response = self.client.post(url, post_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["detail"],
            "No subfolders found",
        )
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Collection.objects.count(), 1)
        batch_announce_delay.assert_not_called()

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_batch_announce_task(self, mock_dispatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)

            bic.process(
                recid="1x3p3-e6505",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            bic.process(
                recid="yz39b-yf220",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            batch_announce_task(batch_announce_folder, self.tag.id, self.user.id)

        self.tag.refresh_from_db()
        self.assertEqual(Archive.objects.count(), 2)
        self.assertEqual(Collection.objects.count(), 1)
        self.assertEqual(self.tag.description, "Batch Announce completed successfully")
        self.assertEqual(mock_dispatch.call_count, 2)
        self.assertEqual(mock_dispatch.mock_calls[0].args[0], Steps.ANNOUNCE)
        self.assertEqual(mock_dispatch.mock_calls[1].args[0], Steps.ANNOUNCE)

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_batch_announce_task_one_validation_failed(self, mock_dispatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)

            sip = bic.process(
                recid="1x3p3-e6505",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            foldername = sip["foldername"]
            path_to_sip = os.path.join(batch_announce_folder, foldername)
            os.remove(os.path.join(path_to_sip, "data/meta/sip.json"))

            bic.process(
                recid="yz39b-yf220",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            batch_announce_task(batch_announce_folder, self.tag.id, self.user.id)

        self.tag.refresh_from_db()
        self.assertEqual(Archive.objects.count(), 1)
        self.assertEqual(Collection.objects.count(), 1)
        self.assertEqual(
            self.tag.description,
            " ERRORS: The given path is not a valid SIP:" + path_to_sip + ".",
        )
        self.assertEqual(mock_dispatch.call_count, 1)
        self.assertEqual(mock_dispatch.mock_calls[0].args[0], Steps.ANNOUNCE)

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_batch_announce_task_all_validation_failed(self, mock_dispatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)

            sip = bic.process(
                recid="njf9e-1q233",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )

            foldername = sip["foldername"]
            path_to_sip = os.path.join(batch_announce_folder, foldername)
            os.remove(os.path.join(path_to_sip, "data/meta/sip.json"))

            sip2 = bic.process(
                recid="1x3p3-e6505",
                source="cds-rdm-sandbox",
                target=batch_announce_folder,
                loglevel=0,
            )
            foldername2 = sip2["foldername"]
            path_to_sip2 = os.path.join(batch_announce_folder, foldername2)
            sip_json = None
            with open(
                os.path.join(path_to_sip2, "data/meta/sip.json"), "r"
            ) as json_file:
                sip_json = json.load(json_file)

            with open(
                os.path.join(path_to_sip2, "data/meta/sip.json"), "w"
            ) as json_file:
                del sip_json["source"]
                json.dump(sip_json, json_file)

            batch_announce_task(batch_announce_folder, self.tag.id, self.user.id)

        self.tag.refresh_from_db()
        self.assertIn("ERRORS:", self.tag.description)
        self.assertIn(
            "The given path is not a valid SIP:" + path_to_sip,
            self.tag.description,
        )
        self.assertIn(
            "Error while reading sip.json:" + path_to_sip2,
            self.tag.description,
        )
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Collection.objects.count(), 1)
        mock_dispatch.asssert_not_called()
