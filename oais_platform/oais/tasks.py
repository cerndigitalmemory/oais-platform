from celery import shared_task
from celery.decorators import task
from bagit_create import main as bic
from time import sleep

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@task(name="process", bind=True)
def process(self, rec_id):
    logger.info("Task started", self)
    res = bic.process(
        recid=rec_id,
        source="cds",
    )
    self.update_state(state="PROGRESS", meta={"bagit_res": res})
