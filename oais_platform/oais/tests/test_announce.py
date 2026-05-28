import logging
import os
import tempfile
from unittest import skip
from unittest.mock import patch

from bagit_create import main as bic
from django.contrib.auth.models import User
from django.urls import reverse
from parameterized import parameterized
from rest_framework import status
from rest_framework.test import APITestCase

from oais_platform.oais.models import (
    Archive,
    Collection,
    Profile,
    Step,
    StepName,
    StepType,
)
from oais_platform.oais.views import check_allowed_path
from oais_platform.settings import BIC_WORKDIR


class AnnounceTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_superuser("user", "", "pw")
        self.client.force_authenticate(user=self.user)

    def test_paths(self):
        self.assertFalse(check_allowed_path("/a/path/123", "standarduser"))
        self.assertFalse(check_allowed_path("/eos/users-a/asvas", "standarduser"))
        self.assertFalse(check_allowed_path("a/dfa/eos", "standarduser"))
        self.assertFalse(check_allowed_path("eos/a/b/c/d/e/", "standarduser"))
        self.assertFalse(check_allowed_path("/eos/home-s/standarduser", "standarduser"))

        self.assertTrue(
            check_allowed_path("/eos/home-s/standarduser/a", "standarduser")
        )
        self.assertTrue(
            check_allowed_path(
                "/eos/user/s/standarduser/announce_folder", "standarduser"
            )
        )

    @skip("Only admins can announce for now")
    def test_announce_wrong_path(self):
        url = reverse("announce")

        post_data = {
            "announce_path": "/eos/user/s/standarduser/announce",
        }

        response = self.client.post(url, post_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["detail"], "You're not allowed to announce this path"
        )
        self.assertEqual(Archive.objects.count(), 0)

    def test_announce_folder_does_not_exist(self):
        url = reverse("announce")

        post_data = {
            "announce_path": "/eos/home-u/user/announce",
        }

        response = self.client.post(url, post_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["detail"],
            "Folder does not exist or the oais user has no access",
        )
        self.assertEqual(Archive.objects.count(), 0)

    @parameterized.expand(
        [
            (False, False),
            (True, False),
            (True, True),
        ]
    )
    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_announce(self, has_collection, collection_exists, mock_dispatch):
        url = reverse("announce")
        if collection_exists:
            system_user = Profile.objects.get(system=True).user
            Collection.objects.create(
                title="Test Collection", creator=system_user, internal=True
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            res = bic.process(
                recid="yz39b-yf220",
                source="cds-rdm-sandbox",
                target=tmpdir,
                loglevel=logging.DEBUG,
                workdir=BIC_WORKDIR,
                collection="Test Collection" if has_collection else None,
            )

            foldername = res["foldername"]
            path_to_sip = os.path.join(tmpdir, foldername)

            post_data = {
                "announce_path": path_to_sip,
            }

            response = self.client.post(url, post_data, format="json")
        latest_archive_id = Archive.objects.latest("id").id
        self.assertRedirects(
            response,
            response.wsgi_request.build_absolute_uri(
                reverse(
                    "archives-detail",
                    kwargs={"pk": latest_archive_id},
                )
            ),
            status_code=302,
        )
        self.assertEqual(Archive.objects.count(), 1)
        archive = Archive.objects.first()
        self.assertEqual(archive.recid, "yz39b-yf220")
        self.assertEqual(archive.source, "cds-rdm-sandbox")
        latest_step = Step.objects.latest("id")
        mock_dispatch.assert_called_once_with(
            StepType.get_by_stepname(StepName.ANNOUNCE),
            latest_archive_id,
            latest_step.id,
            False,
        )
        self.assertEqual(latest_step.input_data_json["announce_path"], path_to_sip)
        self.assertEqual(latest_step.input_data_json["foldername"], foldername)
        self.assertEqual(latest_step.step_type.name, StepName.ANNOUNCE)
        self.assertEqual(latest_step.initiated_by_user, self.user)
        self.assertEqual(latest_step.initiated_by_harvest_batch, None)
        if has_collection:
            collection = Collection.objects.get(title="Test Collection")
            self.assertIn(collection, archive.archive_collections.all())
            self.assertEqual(collection.archives.count(), 1)
            self.assertEqual(archive.archive_collections.count(), 2)
        else:
            self.assertFalse(
                Collection.objects.filter(title="Test Collection").exists()
            )
            self.assertEqual(
                archive.archive_collections.count(), 1
            )  # source collection

    @patch("oais_platform.oais.tasks.pipeline_actions.dispatch_task")
    def test_announce_validation_failed(self, mock_dispatch):
        url = reverse("announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            res = bic.process(
                recid="yz39b-yf220",
                source="cds-rdm-sandbox",
                target=tmpdir,
                loglevel=logging.DEBUG,
                workdir=BIC_WORKDIR,
            )

            foldername = res["foldername"]
            path_to_sip = os.path.join(tmpdir, foldername)

            os.remove(os.path.join(path_to_sip, "data/meta/sip.json"))

            post_data = {
                "announce_path": path_to_sip,
            }

            response = self.client.post(url, post_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["detail"],
            "The given path is not a valid SIP",
        )
        self.assertEqual(Archive.objects.count(), 0)
        mock_dispatch.assert_not_called()
