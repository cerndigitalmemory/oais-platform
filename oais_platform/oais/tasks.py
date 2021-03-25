from celery import shared_task
from celery.decorators import task
from bagit_create import main as bic
from time import sleep

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


@task(bind=True, name="process")
def process(self):
    logger.info("Task started")
    res = bic.process(
        recid=1,
        source="cds",
    )
    self.update_state(state="PROGRESS", meta={"bagit_res": res})
