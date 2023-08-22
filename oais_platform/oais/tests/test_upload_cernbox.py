import json
import ntpath
import os
import tempfile
import zipfile
import shutil
from cmath import log
from unittest import mock
from unittest.mock import patch

from bagit_create import main as bic
from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile, TemporaryUploadedFile
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.test import APITestCase

from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tasks import download_files
from oais_platform.settings import LOCAL_BASE_PATH, FILE_LIMIT


class UploadCERNBoxTests(APITestCase):
    def test_file_count_limit(self):
        url = reverse("upload-cernbox")
        response = self.client.post(url, UploadCERNBoxTests.get_public_links(FILE_LIMIT + 1))

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        # No archives and no step should've been created
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Step.objects.count(), 0)

    def test_file_download(self):
        file_list = UploadCERNBoxTests.get_public_links(5)
        subfolder_name = download_files(json.dumps(file_list))
        path = os.path.join(LOCAL_BASE_PATH, subfolder_name)

        for file_name in file_list.keys():
            self.assertEqual(os.path.exists(os.path.join(path, file_name)), True)

        shutil.rmtree(path)

    @staticmethod
    def get_public_links(file_count):
        """
        Creates a mock JSON file that contains `file_count` number of public links.
        """
        FILE_PUBLIC_LINK = "https://gitlab.cern.ch/digitalmemory/oais-platform/-/raw/develop/README.md"
        files = {}

        for i in range(file_count):
            file_name = f"file{i}.md"
            files[file_name] = FILE_PUBLIC_LINK

        return files
