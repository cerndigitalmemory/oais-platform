from django.contrib.auth.models import Permission, User
from oais_platform.oais.models import Archive, Steps, Step, Status
from rest_framework.test import APITestCase, override_settings
#from django.test import SimpleTestCase,TestCase, override_settings, modify_settings
from oais_platform.oais.tests.utils import get_sample_sip_json
from oais_platform.oais import tasks
from unittest import mock
from unittest.mock import patch
from os.path import join
import tempfile, os, bagit_create, oais_utils
from celery import shared_task, states
from celery.utils.log import get_task_logger
from celery.contrib.testing.worker import start_worker

from django.conf import settings
from oais_platform.celery import app
# import pytest


class TaskTests(APITestCase):

    def setUp(self):
        self.permission = Permission.objects.get(codename="can_access_all_archives")

        self.creator = User.objects.create_user("creator", password="pw")
        self.other_user = User.objects.create_user("other", password="pw")

        self.archive = Archive.objects.create(
            recid="1", source="test", source_url="", creator=self.creator
        )
    
    @override_settings(CELERY_TASK_ALWAYS_EAGER=True,CELERY_TASK_EAGER_PROPOGATES=True,BIC_UPLOAD_PATH='test')
    @patch("bagit_create.main.process")
    def test_harvest(self, bagit_mock):
        bagit_mock.return_value = {'status': 0,'foldername': "test", 'errormsg': None}

        step = tasks.create_step(Steps.HARVEST, self.archive.id)
        self.task = tasks.process.delay(self.archive.id, step.id)

        self.bagit_result = self.task.get()
        bagit_mock.assert_called()
        
        self.assertEqual(step.status, Status.WAITING)
        self.archive.refresh_from_db()
        step.refresh_from_db()

        self.assertEqual(self.bagit_result, {'status': 0,'foldername': "test", 'errormsg': None, 'artifact': 
            {'artifact_name': 'SIP','artifact_path': f"/oais-data/sip/{settings.BIC_UPLOAD_PATH}/test",'artifact_url': f'https://oais.web.cern.ch/oais-data/sip/{settings.BIC_UPLOAD_PATH}/test'}})
        self.assertEqual(step.name, Steps.HARVEST)
        self.assertEqual(step.status, Status.COMPLETED)
        self.assertEqual(self.archive.path_to_sip, join(settings.BIC_UPLOAD_PATH,self.bagit_result["foldername"]))


    @override_settings(CELERY_TASK_ALWAYS_EAGER=True,CELERY_TASK_EAGER_PROPOGATES=True)
    @patch("bagit_create.main.process")
    def test_harvest_failed(self, bagit):
        bagit.return_value = {'status': 1,'errormsg': "Failed"}

        step = tasks.create_step(Steps.HARVEST, self.archive.id)

        self.task = tasks.process.delay(self.archive.id, step.id)
        self.bagit_result = self.task.get()
        bagit.assert_called()
        
        self.assertEqual(step.status, Status.WAITING)
        self.archive.refresh_from_db()
        step.refresh_from_db()

        self.assertEqual(self.bagit_result, {'status': 1, 'errormsg': "Failed"})
        self.assertEqual(step.name, Steps.HARVEST)
        self.assertEqual(step.status, Status.FAILED)

    @override_settings(CELERY_TASK_ALWAYS_EAGER=True,CELERY_TASK_EAGER_PROPOGATES=True)
    @patch("oais_utils.validate.validate_sip")
    def test_validate(self, validate_sip):
        with tempfile.TemporaryDirectory() as test_dir:
            self.archive.set_path(test_dir)

            validate_sip.return_value = True

            step = tasks.create_step(Steps.VALIDATION, self.archive.id)

            self.task = tasks.validate.delay(self.archive.id, step.id)
            self.validation_result = self.task.get()
            validate_sip.assert_called()
            
            self.assertEqual(step.status, Status.WAITING)
            self.archive.refresh_from_db()
            step.refresh_from_db()

            self.assertEqual(self.validation_result, {'status': 0, 'errormsg': None, 'foldername': test_dir})
            self.assertEqual(step.name, Steps.VALIDATION)
            self.assertEqual(step.status, Status.COMPLETED)

    @override_settings(CELERY_TASK_ALWAYS_EAGER=True,CELERY_TASK_EAGER_PROPOGATES=True)
    @patch("oais_utils.validate.validate_sip")
    def test_validate_fail(self, validate_sip):
        with tempfile.TemporaryDirectory() as test_dir:
            self.archive.set_path(test_dir)

            validate_sip.return_value = False

            step = tasks.create_step(Steps.VALIDATION, self.archive.id)

            self.task = tasks.validate.delay(self.archive.id, step.id, input_data=None)
            self.validation_result = self.task.get()
            
            validate_sip.assert_called()
            
            self.assertEqual(step.status, Status.WAITING)
            self.archive.refresh_from_db()
            step.refresh_from_db()

            self.assertEqual(self.validation_result, {'status': 0, 'errormsg': None, 'foldername': test_dir})
            self.assertEqual(step.name, Steps.VALIDATION)
            self.assertEqual(step.status, Status.COMPLETED)

    @override_settings(CELERY_TASK_ALWAYS_EAGER=True,CELERY_TASK_EAGER_PROPOGATES=True)
    @patch("oais_utils.validate.validate_sip")
    def test_validate_non_existant(self, validate_sip):
        validate_sip.return_value = False

        step = tasks.create_step(Steps.VALIDATION, self.archive.id)

        self.task = tasks.validate.delay(self.archive.id, step.id, input_data=None)
        self.validation_result = self.task.get()

        validate_sip.assert_not_called()
        
        self.assertEqual(step.status, Status.WAITING)
        self.archive.refresh_from_db()
        step.refresh_from_db()

        self.assertEqual(self.validation_result, {"status": 1, "errormsg": "SIP folder was not found"})
        self.assertEqual(step.name, Steps.VALIDATION)
        self.assertEqual(step.status, Status.FAILED)
    
    def test_checksum_failed_json(self):
        """
        Tests checksum with an empty json file
        """
        with tempfile.TemporaryDirectory() as test_dir:
            self.archive.set_path(test_dir)
            temp_sip_path = f"{test_dir}/data/meta/"

            os.makedirs(temp_sip_path)
            with open(f"{temp_sip_path}/sip.json", 'w') as fp:

                step = tasks.create_step(Steps.CHECKSUM, self.archive.id)

                checksum_result = tasks.checksum(self.archive.id, step.id, input_data=None)

            self.assertEqual(step.status, Status.WAITING)
            self.archive.refresh_from_db()
            step.refresh_from_db()

            self.assertEqual(checksum_result,  {"status": 1, "errormsg": "sip.json file error: Expecting value: line 1 column 1 (char 0)"})
            self.assertEqual(step.name, Steps.CHECKSUM)
            self.assertEqual(step.status, Status.IN_PROGRESS)

    def test_checksum_non_existant(self):
        step = tasks.create_step(Steps.CHECKSUM, self.archive.id)

        checksum_result = tasks.checksum(self.archive.id, step.id, input_data=None)

        self.archive.refresh_from_db()

        self.assertEqual(checksum_result, {'status': 1, "errormsg": "SIP folder was not found"})
        self.assertEqual(step.name, Steps.CHECKSUM)
        self.assertEqual(step.status, Status.WAITING)
    
    
    def test_checksum(self):
        with tempfile.TemporaryDirectory() as test_dir:
            self.archive.set_path(test_dir)
            temp_sip_path = f"{test_dir}/data/meta/"

            os.makedirs(temp_sip_path)
            with open(f"{temp_sip_path}/sip.json", 'w') as fp:
                fp.write(get_sample_sip_json())

            step = tasks.create_step(Steps.CHECKSUM, self.archive.id)
         
            with open(f"{temp_sip_path}/sip.json", 'r+') as fp:
                checksum_result = tasks.checksum(self.archive.id, step.id, input_data=None)

            self.assertEqual(step.status, Status.WAITING)
            self.archive.refresh_from_db()
            step.refresh_from_db()

            self.assertEqual(checksum_result,  {"status": 0, "errormsg": None, "foldername": test_dir})
            self.assertEqual(step.name, Steps.CHECKSUM)
            self.assertEqual(step.status, Status.IN_PROGRESS)
    
    @override_settings(CELERY_TASK_ALWAYS_EAGER=True,CELERY_TASK_EAGER_PROPOGATES=True, BIC_UPLOAD_PATH='test')
    @patch("django_celery_beat.models.PeriodicTask.objects.create")
    def test_archivematica(self, periodic_task):
        with tempfile.TemporaryDirectory(dir=settings.BIC_UPLOAD_PATH) as test_dir:
            self.archive.set_path(test_dir)
            with tempfile.TemporaryFile(dir=test_dir) as fp:

                step = tasks.create_step(Steps.ARCHIVE, self.archive.id)

                self.task = tasks.archivematica.delay(self.archive.id, step.id, input_data=None)
                self.am_result = self.task.get()
                
                periodic_task.assert_called()

        self.archive.refresh_from_db()
        step.refresh_from_db()

        self.assertEqual(self.am_result, {"status": 0, "message": "Uploaded to Archivematica"})
        self.assertEqual(step.name, Steps.ARCHIVE)
        self.assertEqual(step.status, Status.WAITING)
    
    
    """
    When a celery task is called we need to find a way to override the settings in order to check error handling in case
    of wrong archivematica configuration, username, password etc. Right now settings are overriden in the test but not in celery.
    """
    @override_settings(CELERY_TASK_ALWAYS_EAGER=True,CELERY_TASK_EAGER_PROPOGATES=True,BIC_UPLOAD_PATH='test',AM_URL="wrong_url.cern.ch")
    def test_archivematica_wrong_config(self):
        with tempfile.TemporaryDirectory(dir=settings.BIC_UPLOAD_PATH) as test_dir:
            self.archive.set_path(test_dir)
            with tempfile.TemporaryFile(dir=test_dir) as fp:

                step = tasks.create_step(Steps.ARCHIVE, self.archive.id)

                am_result = tasks.archivematica(self.archive.id, step.id, input_data=None)
                
                periodic_task.assert_called()

        self.archive.refresh_from_db()
        step.refresh_from_db()
        print(am_result)

        self.assertEqual(am_result, {"status": 1, "message": f"Error while archiving {step.id}. Check your archivematica settings configuration."})
        self.assertEqual(step.name, Steps.ARCHIVE)
        self.assertEqual(step.status, Status.WAITING)





    

        
        