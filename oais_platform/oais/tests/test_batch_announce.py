import tempfile
import os
import json

from django.contrib.auth.models import User
from django.urls import reverse
from django.test import override_settings

from rest_framework.test import APITestCase
from rest_framework import status
from bagit_create import main as bic

from oais_platform.oais.models import Collection, Archive


class BatchAnnounceTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)

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
        self.assertEqual(Collection.objects.count(), 0)

    def test_batch_announce_duplicate_tag(self):
        url = reverse("batch-announce")

        self.collection = Collection.objects.create(
            title="test_tag", internal=False, creator=self.user
        )

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
        self.assertEqual(Collection.objects.count(), 1)

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
        self.assertEqual(Collection.objects.count(), 0)

    def test_batch_announce(self):
        self.user.is_superuser = True
        self.user.save()

        url = reverse("batch-announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)
            # Test that file does not affect subfolder count
            with open(os.path.join(batch_announce_folder, "empty_file"), "w"):
                pass

            bic.process(
                recid="2728246",
                source="cds",
                target=batch_announce_folder,
                loglevel=0,
            )

            bic.process(
                recid="2779856",
                source="cds",
                target=batch_announce_folder,
                loglevel=0,
            )

            post_data = {
                "batch_announce_path": batch_announce_folder,
                "batch_tag": "test_tag",
            }

            with override_settings(BATCH_ANNOUNCE_LIMIT=2):
                response = self.client.post(url, post_data, format="json")
                tag_id = Collection.objects.latest("id").id
                self.assertRedirects(
                    response, reverse("tags-detail", kwargs={"pk": tag_id}), status_code=302
                )
                self.assertEqual(Archive.objects.count(), 2)
                self.assertEqual(Collection.objects.count(), 1)
                self.assertEqual(
                    Collection.objects.get(id=tag_id).description,
                    "Batch announce successful",
                )

    def test_batch_announce_limit_exceeded(self):
        self.user.is_superuser = True
        self.user.save()

        url = reverse("batch-announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)

            bic.process(
                recid="2728246",
                source="cds",
                target=batch_announce_folder,
                loglevel=0,
            )

            bic.process(
                recid="2779856",
                source="cds",
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
                self.assertEqual(Collection.objects.count(), 0)

    def test_batch_announce_validation_failed(self):
        self.user.is_superuser = True
        self.user.save()

        url = reverse("batch-announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)

            sip = bic.process(
                recid="2728246",
                source="cds",
                target=batch_announce_folder,
                loglevel=0,
            )

            foldername = sip["foldername"]
            path_to_sip = os.path.join(batch_announce_folder, foldername)
            os.remove(os.path.join(path_to_sip, "data/meta/sip.json"))

            bic.process(
                recid="2779856",
                source="cds",
                target=batch_announce_folder,
                loglevel=0,
            )

            post_data = {
                "batch_announce_path": batch_announce_folder,
                "batch_tag": "test_tag",
            }

            response = self.client.post(url, post_data, format="json")
            tag_id = Collection.objects.latest("id").id
            self.assertRedirects(
                response, reverse("tags-detail", kwargs={"pk": tag_id}), status_code=302
            )
            self.assertEqual(Archive.objects.count(), 1)
            self.assertEqual(Collection.objects.count(), 1)
            self.assertEqual(
                Collection.objects.get(id=tag_id).description,
                "Failed SIP folders: "
                + path_to_sip
                + " - "
                + "The given path is not a valid SIP."
                + " ",
            )

    def test_batch_announce_all_validation_failed(self):
        self.user.is_superuser = True
        self.user.save()

        url = reverse("batch-announce")

        with tempfile.TemporaryDirectory() as tmpdir:
            batch_announce_folder = os.path.join(tmpdir, "sips")
            os.mkdir(batch_announce_folder)

            sip = bic.process(
                recid="2728246",
                source="cds",
                target=batch_announce_folder,
                loglevel=0,
            )

            foldername = sip["foldername"]
            path_to_sip = os.path.join(batch_announce_folder, foldername)
            os.remove(os.path.join(path_to_sip, "data/meta/sip.json"))

            sip2 = bic.process(
                recid="2779856",
                source="cds",
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

            post_data = {
                "batch_announce_path": batch_announce_folder,
                "batch_tag": "test_tag",
            }

            response = self.client.post(url, post_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
            self.assertIn("Failed SIP folders: ", response.data["detail"])
            self.assertIn(
                path_to_sip + " - " + "The given path is not a valid SIP." + " ",
                response.data["detail"],
            )
            self.assertIn(
                path_to_sip2 + " - " + "Error while reading sip.json" + " ",
                response.data["detail"],
            )
            self.assertEqual(Archive.objects.count(), 0)
            self.assertEqual(Collection.objects.count(), 0)
