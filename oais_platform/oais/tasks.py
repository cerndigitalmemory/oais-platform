from bagit_create import main as bic
from celery import states
from celery.decorators import task
from celery.utils.log import get_task_logger
from oais_platform.oais.models import Archive

logger = get_task_logger(__name__)


def process_after_return(self, status, retval, task_id, args, kwargs, einfo):
    # archive_id is the first parameter passed to the task
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)
    if status == states.SUCCESS:
        if retval["status"] == 0:
            archive.set_completed()
        else:
            # bagit_create returned an error
            errormsg = retval["errormsg"]
            logger.error(
                f"Error while harvesting archive {archive_id}: {errormsg}")
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
        ark_json=False,
        ark_json_rel=False,
    )

    return bagit_result
