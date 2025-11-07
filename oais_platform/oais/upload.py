import logging
import re
import urllib

from oais_platform.oais.models import Status


def sanitize_filename(filename):
    """
    Converts filename to be able to be safely processed in the pipeline (like Archivematica).
    """
    filename = urllib.parse.unquote(filename)
    if re.search(r"[/\x00-\x1F\U00010000-\U0010FFFF]", filename):
        logging.warning("Filename with invalid characters detected. Sanitizing.")
        filename = re.sub(r"[/\x00-\x1F]", "-", filename)
        filename = re.sub(r"[^\u0000-\uFFFF]", "?", filename)
    return filename


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
