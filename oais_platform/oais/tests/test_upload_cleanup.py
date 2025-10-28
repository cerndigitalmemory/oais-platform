import os
import shutil
import tempfile
import time
from unittest.mock import patch

from rest_framework.test import APITestCase

from oais_platform.oais.tasks.create_sip import (
    UPLOAD_DELETION_CUTOFF_DAYS,
    upload_cleanup,
)


class UploadCleanupTaskTest(APITestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

        self.current_time = time.time()
        self.cutoff_time = self.current_time - (
            UPLOAD_DELETION_CUTOFF_DAYS * 24 * 60 * 60
        )

        self.old_upload = os.path.join(self.temp_dir, "old_upload")
        self.new_upload = os.path.join(self.temp_dir, "new_upload")
        os.makedirs(self.old_upload)
        os.makedirs(self.new_upload)

        self.old_file_path = os.path.join(self.old_upload, "old_file.txt")
        with open(self.old_file_path, "w") as f:
            f.write("Old uploaded file that should be deleted.")
        os.utime(self.old_file_path, (self.cutoff_time - 1, self.cutoff_time - 1))

        self.new_file_path = os.path.join(self.new_upload, "new_file.txt")
        with open(self.new_file_path, "w") as f:
            f.write("New uploaded file that should be kept.")

        self.path_patch = patch(
            "oais_platform.oais.tasks.create_sip.LOCAL_UPLOAD_PATH", self.temp_dir
        )
        self.path_patch.start()

    def tearDown(self):
        self.path_patch.stop()
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("oais_platform.oais.tasks.create_sip.time.time")
    def test_upload_cleanup_success(self, mock_time):
        mock_time.return_value = self.current_time

        upload_cleanup.apply()

        self.assertFalse(os.path.exists(self.old_file_path))
        self.assertFalse(os.path.exists(self.old_upload))

        self.assertTrue(os.path.exists(self.new_file_path))
        self.assertTrue(os.path.exists(self.new_upload))
        self.assertTrue(os.path.exists(self.temp_dir))
