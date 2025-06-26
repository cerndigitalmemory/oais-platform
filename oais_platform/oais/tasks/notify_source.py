from celery import shared_task
from celery.utils.log import get_task_logger

from oais_platform.oais.exceptions import RetryableException
from oais_platform.oais.models import Archive, ArchiveState, Source, Status, Step
from oais_platform.oais.sources.utils import get_source
from oais_platform.oais.tasks.pipeline_actions import finalize

# Logger to be used inside Celery tasks
logger = get_task_logger(__name__)
logger.setLevel("DEBUG")


@shared_task(
    name="notify_source",
    bind=True,
    ignore_result=True,
    after_return=finalize,
    max_retries=5,
)
def notify_source(self, archive_id, step_id, input_data=None, api_key=None):
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    logger.info(
        f"Starting to notify the upstream source({archive.source}) for Archive {archive.id}"
    )

    if archive.state != ArchiveState.AIP:
        return {"status": 1, "errormsg": f"Archive {archive.id} is not an AIP."}

    try:
        source = Source.objects.get(name=archive.source)
    except Source.DoesNotExist:
        return {
            "status": 1,
            "errormsg": f"Source object with name {archive.source} does not exist.",
        }
    if not source.notification_enabled:
        return {
            "status": 1,
            "errormsg": f"Notify source disabled for {archive.source}.",
        }
    if not source.notification_endpoint or len(source.notification_endpoint) == 0:
        return {
            "status": 1,
            "errormsg": f"Archive's source ({archive.source}) has no notification endpoint set.",
        }

    try:
        get_source(archive.source).notify_source(
            archive, source.notification_endpoint, api_key
        )
        return {
            "status": 0,
            "errormsg": None,
        }
    except RetryableException as e:
        self.retry(exc=e, countdown=60)
    except Exception as e:
        return {
            "status": 1,
            "errormsg": str(e),
        }
