import logging
import re
import urllib

from oais_platform.oais.models import Status


def handle_failed_upload(archive, step, error_msg):
    logging.error(error_msg)
    step.set_status(Status.FAILED)
    step.set_output_data(
        {
            "status": 1,
            "errormsg": error_msg,
            "archive": archive.id,
        }
    )
    archive.set_last_step(step.id)
