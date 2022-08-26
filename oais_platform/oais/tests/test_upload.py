import tempfile
import os
import json
import shutil
import zipfile

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from bagit_create import main as bic

from oais_platform.oais.models import User, UploadJob, Archive, Step, Steps, Status
from oais_platform.oais.tasks import build_sip, uncompress


class UploadTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)

        # set up a tmp directory
        tmp_dir = "/oais_platform/tmp/test"
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir)

        # create a dummy UploadJob, Archive and Step
        self.uj = UploadJob.objects.create(
            creator=self.user,
            tmp_dir=tmp_dir,
            files=json.dumps({})
        )

        self.archive = Archive.objects.create(
            recid="", source="local", source_url="", creator=self.user
        )

        self.step = Step.objects.create(
            archive=self.archive, name=Steps.SIP_UPLOAD, status=Status.IN_PROGRESS
        )

    def test_create_job(self):
        """
        Asserts that the a new entry in the UploadJob table gets inserted \n
        and that its corresponding tmp dir exists
        """
        num_jobs = UploadJob.objects.count()

        url = reverse("upload-create-job")
        response = self.client.post(url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(UploadJob.objects.count(), num_jobs + 1)

        uj = UploadJob.objects.get(pk=response.data["uploadJobId"])
        self.assertEqual(os.path.exists(uj.tmp_dir), True)

        # delete the tmp dir and the UploadJob
        shutil.rmtree(uj.tmp_dir)
        uj.delete()

    def test_add_file(self):
        """
        Asserts a file got added to the mock UploadJob: the file is under "/oais_platform/tmp/test"
        and its name is in the files data field of the mock UploadJob entry.
        """
        with tempfile.NamedTemporaryFile() as tf:
            tf_name = os.path.basename(tf.name)

            url = reverse("upload-add-file", args=[self.uj.id])
            response = self.client.post(url, data={tf_name: tf})

            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(len(os.listdir(self.uj.tmp_dir)), 1)

            # assert the data field files gets updated correctly
            self.uj.add_file(os.path.join(self.uj.tmp_dir, tf_name), tf_name)
            name_in_db = self.uj.get_files().values().__iter__().__next__()
            self.assertEqual(name_in_db, os.path.basename(tf.name))

    def test_build_sip(self):
        """
        Asserts SIPs get built correctly
        """
        with tempfile.TemporaryDirectory() as td:
            with tempfile.NamedTemporaryFile(dir=td):
                try:
                    res = build_sip(self.archive, td, td)
                except Exception as e:
                    self.fail(f"build_sip() raised an exception: {str(e)}")

                self.assertEqual(res[0] in os.listdir(td), True)

    def test_uncompress(self):
        """
        Asserts SIPs get uncompressed and moved correctly
        """
        with tempfile.TemporaryDirectory() as sip_td:
            # create an SIP to compress
            res = bic.process(
                recid="2728246",
                source="cds",
                target=sip_td,
                loglevel=0,
            )

            sip_folder_name = res["foldername"]

            # a separate dir is needed, ow we walk a dir that keeps getting a zip updated
            with tempfile.TemporaryDirectory() as zip_td:
                # compress the SIP
                with zipfile.ZipFile(os.path.join(zip_td, "test.zip"), "x") as zip_obj:
                    for root, subfolders, files in os.walk(sip_td):
                        for file in files:
                            file_path = os.path.join(root, file)
                            zip_obj.write(file_path, os.path.basename(file_path))
                zip_obj.close()

                try:
                    res = uncompress(zip_td, sip_td)
                except Exception as e:
                    self.fail(f"uncomrpess() raised an exception: {str(e)}")

                self.assertEqual(sip_folder_name in os.listdir(sip_td), True)

        # this is the last test, dleete the tmp dir of the mock uj
        if os.path.exists(self.uj.tmp_dir):
            shutil.rmtree(self.uj.tmp_dir)
