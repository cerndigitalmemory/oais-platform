import tempfile
import os
from unittest import skip
from unittest.mock import patch

from django.contrib.auth.models import User
from django.urls import reverse

from rest_framework.test import APITestCase
from rest_framework import status
from bagit_create import main as bic

from oais_platform.oais.views import check_allowed_path
from oais_platform.oais.models import Archive


class AnnounceTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.user.is_superuser = True
        self.user.save()
        self.client.force_authenticate(user=self.user)

        patch("celery.app.task.Task.delay", return_value=1)
        patch("celery.app.task.Task.apply_async", return_value=1)

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

    def test_announce(self):
        url = reverse("announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            res = bic.process(
                recid="2728246",
                source="cds",
                target=tmpdir,
                loglevel=0,
            )

            foldername = res["foldername"]
            path_to_sip = os.path.join(tmpdir, foldername)

            post_data = {
                "announce_path": path_to_sip,
            }

            response = self.client.post(url, post_data, format="json")
            self.assertRedirects(
                response,
                response.wsgi_request.build_absolute_uri(
                    reverse("archives-sgl-details", kwargs={"pk": Archive.objects.latest("id").id})
                ),
                status_code=302
            )
            self.assertEqual(Archive.objects.count(), 1)

    def test_announce_validation_failed(self):
        url = reverse("announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            res = bic.process(
                recid="2728246",
                source="cds",
                target=tmpdir,
                loglevel=0,
            )

            foldername = res["foldername"]
            path_to_sip = os.path.join(tmpdir, foldername)

            os.remove(os.path.join(path_to_sip, "data/meta/sip.json"))

            post_data = {
                "announce_path": path_to_sip,
            }

            response = self.client.post(url, post_data, format="json")
            print(response)
            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
            self.assertEqual(
                response.data["detail"],
                "The given path is not a valid SIP.",
            )
            self.assertEqual(Archive.objects.count(), 0)
