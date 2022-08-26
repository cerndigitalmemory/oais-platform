import os
import json
import tempfile
import datetime

from oais_platform.oais.models import User, UploadJob, UJStatus
from oais_platform.oais.services import uploadjob_cleanup
from oais_platform.settings import JOB_EXPIRY_TIME

from rest_framework.test import APITestCase
from django.core.exceptions import ObjectDoesNotExist
from django.utils import timezone


class UJCleanupTest(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user("user", "", "pw")
        self.client.force_authenticate(user=self.user)

        self.tmp_dir = tempfile.TemporaryDirectory().name

    # Note: can't use tempfile.TemporaryDirectory() under context for these tests
    # they get deleted before exiting the context and thus, error is thrown when context ends

    def test_method_success(self):
        """
        Asserts that a successful UJ gets cleaned up correctly
        """
        uj = UploadJob.objects.create(
            creator=self.user,
            tmp_dir=self.tmp_dir,
            files=json.dumps({}),
            status=UJStatus.SUCCESS
        )
        id = uj.id

        uploadjob_cleanup()

        self.assertRaises(ObjectDoesNotExist, UploadJob.objects.get, pk=id)
        self.assertEqual(os.path.exists(self.tmp_dir), False)

    def test_method_fail(self):
        """
        Asserts that a failed UJ gets cleaned up correctly
        """
        os.makedirs(self.tmp_dir)

        uj = UploadJob.objects.create(
            creator=self.user,
            tmp_dir=self.tmp_dir,
            files=json.dumps({}),
            status=UJStatus.FAIL
        )
        id = uj.id

        uploadjob_cleanup()

        self.assertRaises(ObjectDoesNotExist, UploadJob.objects.get, pk=id)
        self.assertEqual(os.path.exists(self.tmp_dir), False)

    def test_method_pending_not_expired(self):
        """
        Asserts that a pending, not expired, UJ does not get deleted
        """
        os.makedirs(self.tmp_dir)

        uj = UploadJob.objects.create(
            creator=self.user,
            tmp_dir=self.tmp_dir,
            files=json.dumps({}),
            status=UJStatus.PENDING
        )
        id = uj.id

        uploadjob_cleanup()

        try:
            UploadJob.objects.get(pk=id)
        except ObjectDoesNotExist:
            self.fail("The upload job got deleted too soon")

        self.assertEqual(os.path.exists(self.tmp_dir), True)

    def test_method_pending_expired(self):
        """
        Asserts that a pending, expired, UJ does not get deleted
        """
        expired_timestamp = timezone.now() - datetime.timedelta(hours=int(JOB_EXPIRY_TIME))

        uj = UploadJob.objects.create(
            creator=self.user,
            timestamp=expired_timestamp,
            tmp_dir=self.tmp_dir,
            files=json.dumps({}),
            status=UJStatus.PENDING
        )
        id = uj.id

        uploadjob_cleanup()

        self.assertRaises(ObjectDoesNotExist, UploadJob.objects.get, pk=id)
        self.assertEqual(os.path.exists(self.tmp_dir), False)
