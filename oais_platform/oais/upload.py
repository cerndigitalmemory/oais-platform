import logging
import re
import urllib


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
