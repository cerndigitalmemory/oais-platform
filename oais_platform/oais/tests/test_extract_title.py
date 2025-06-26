import os
import shutil
import tempfile

from bagit_create import main as bic
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Step
from oais_platform.oais.tasks.extract_title import extract_title


class ExtractTitleTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(
            recid="1", source="test", source_url="", title=""
        )

        self.step = Step.objects.create(archive=self.archive, name=10)

    def test_extract_title_no_dc(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            res = bic.process(
                recid="njf9e-1q233",
                source="cds-rdm-sandbox",
                target=tmpdir,
                loglevel=0,
            )

            foldername = res["foldername"]
            path_to_sip = os.path.join(tmpdir, foldername)
            self.archive.set_path(path_to_sip)

            result = extract_title(self.archive.id, self.step.id)

            self.assertEqual(result["status"], 1)
            self.assertEqual(self.archive.title, "")

    def test_extract_title_success(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            res = bic.process(
                recid="njf9e-1q233",
                source="cds-rdm-sandbox",
                target=tmpdir,
                loglevel=0,
            )

            foldername = res["foldername"]
            path_to_sip = os.path.join(tmpdir, foldername)
            self.archive.set_path(path_to_sip)

            # Add dc.xml
            current_dir = os.path.dirname(os.path.abspath(__file__))
            dc_path = os.path.join(current_dir, "data/dc.xml")
            shutil.copyfile(dc_path, os.path.join(path_to_sip, "data/meta/dc.xml"))

            result = extract_title(self.archive.id, self.step.id)
            self.archive.refresh_from_db()
            self.assertEqual(result["status"], 0)
            self.assertEqual(self.archive.title, "TEST TITLE")
