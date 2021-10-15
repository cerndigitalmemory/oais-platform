from bagit_create import main as bic
from celery import states
from celery.decorators import task
from celery.utils.log import get_task_logger
from oais_platform.oais.models import Archive

#import oais_utils
import time

logger = get_task_logger(__name__)


def process_after_return(self, status, retval, task_id, args, kwargs, einfo):
    # archive_id is the first parameter passed to the task
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)
    if status == states.SUCCESS:

        if retval["status"] == 0:
            # archive.set_completed()
            archive.set_sip_exists()
            # TODO set path from bic return? for validation
            archive.path_to_sip = "path"
        else:
            # bagit_create returned an error
            errormsg = retval["errormsg"]
            logger.error(f"Error while harvesting archive {archive_id}: {errormsg}")
            archive.set_failed()
    else:
        archive.set_failed()


@task(name="process", bind=True, ignore_result=True, after_return=process_after_return)
def process(self, archive_id):
    logger.info(f"Starting harvest of archive {archive_id}")

    archive = Archive.objects.get(pk=archive_id)
    archive.set_in_progress(self.request.id)

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
    if status == states.SUCCESS:
        if retval:
            archive.set_completed()
            archive.set_valid_sip()
        else:
            logger.error(
                f"Error while validating sip {archive_id}")
            archive.set_failed()
    else:
        archive.set_failed()

@task(name="validate", bind=True, ignore_result=True, after_return=validate_after_return)
def validate(self, archive_id, path_to_sip):
    logger.info(f"Starting SIP validation {path_to_sip}")

    archive = Archive.objects.get(pk=archive_id)
    archive.set_in_progress(self.request.id)

    # TODO: Run validation 
    # valid = oais_utils.validate.validate_aip(path_to_bag)
    
    # MOCK
    valid = True
    time.sleep(5)

    return valid

