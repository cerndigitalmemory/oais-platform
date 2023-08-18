import json
import ntpath
import os
import tempfile
import zipfile
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
from oais_platform.oais.tests.utils import TestSource
from oais_platform.settings import LOCAL_BASE_PATH, FILE_LIMIT


class UploadCERNBoxTests(APITestCase):
    def test_file_count_limit(self):
        url = reverse("upload-cernbox")
        response = self.client.post(url, TestSoruce.get_public_links_for_download(FILE_LIMIT + 1))

        self.assertEqual(resonse.status_code, status.HTTP_400_BAD_REQUEST)
        # No archives and not steps should've been created
        self.assertEqual(Archive.objects.count(), 0)
        self.assertEqual(Step.objects.count(), 0)
