from django import utils
from django.db.models import base
from bagit_create import main as bic
from celery import states

# import oais_utils
from celery.decorators import task
from celery.utils.log import get_task_logger
from oais_platform.oais.models import Archive, Job, Stages, Status
from django.utils import timezone
from oais_utils import oais_utils

# import oais_utils
import time, os, zipfile

logger = get_task_logger(__name__)


def process_after_return(self, status, retval, task_id, args, kwargs, einfo):
    # archive_id is the first parameter passed to the task
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)
    logger.info(f"Archive {archive}")
    job_id = args[1]
    job = Job.objects.get(pk=job_id)
    if status == states.SUCCESS:

        if retval["status"] == 0:
            # TODO fix path
            filename = f"bagitexport::{archive.record.source}::{archive.record.recid}"
            archive.path_to_sip = os.path.join(os.getcwd(), filename)

            # Previous job
            job.set_completed()

            # Next job
            registry_job = Job.objects.create(
                archive=archive, stage=Stages.CHECKING_REGISTRY, status=Status.PENDING
            )

            # New Celery task will start
            archive.set_pending()
            valid = validate.delay(archive.id, archive.path_to_sip, registry_job.id)
            if valid:
                registry_job.set_completed()
                archive.set_completed()
            else:
                job.set_failed()
                archive.set_failed()
        else:
            # bagit_create returned an error
            errormsg = retval["errormsg"]
            logger.error(f"Error while harvesting archive {archive_id}: {errormsg}")
            job.set_failed()
            archive.set_failed()
    else:
        job.set_failed()
        archive.set_failed()


@task(name="process", bind=True, ignore_result=True, after_return=process_after_return)
def process(self, archive_id, job_id):
    logger.info(f"Starting harvest of archive {archive_id}")

    archive = Archive.objects.get(pk=archive_id)
    archive.set_in_progress(self.request.id)

    job = Job.objects.get(pk=job_id)
    job.set_in_progress(self.request.id)

    bagit_result = bic.process(
        recid=archive.record.recid,
        source=archive.record.source,
        loglevel=2,
    )

    return bagit_result


def validate_after_return(self, status, retval, task_id, args, kwargs, einfo):
    # archive_id is the first parameter passed to the task
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)

    # Could be failed registry_check/validation or successful validation
    job = archive.get_latest_job()
    if status == states.SUCCESS:
        if retval:
            archive.set_completed()
            job.set_completed()
        else:
            logger.error(f"Error while validating sip {archive_id}")
            job.set_failed()
            archive.set_failed()
    else:
        job.set_failed()
        archive.set_failed()


@task(
    name="validate", bind=True, ignore_result=True, after_return=validate_after_return
)
def validate(self, archive_id, path_to_sip, job_id):
    logger.info(f"Starting SIP validation {path_to_sip}")

    registry_job = Job.objects.get(pk=job_id)
    registry_job.set_in_progress(self.request.id)

    archive = Archive.objects.get(pk=archive_id)
    archive.set_in_progress(self.request.id)

    # Checking registry = checking if the folder exists
    sip_exists = os.path.exists(path_to_sip)

    if not sip_exists:
        return True

    registry_job.set_completed()

    # Next job
    validation_job = Job.objects.create(
        archive=archive,
        stage=Stages.VALIDATION,
        status=Status.IN_PROGRESS,
        celery_task_id=self.request.id,
    )

    # TODO: Run validation
    valid = oais_utils.validate.validate_sip(path_to_sip)

    # MOCK
    time.sleep(5)

    return valid
