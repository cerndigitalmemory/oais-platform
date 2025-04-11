import logging

from django.apps import AppConfig

from oais_platform.settings import FTS_GRID_CERT, FTS_GRID_CERT_KEY, FTS_INSTANCE

from .fts import FTS

fts = None


class OaisConfig(AppConfig):
    name = "oais_platform.oais"

    def ready(self):
        global fts
        logging.getLogger("fts3.rest.client").setLevel(logging.DEBUG)
        try:
            fts = FTS(FTS_INSTANCE, FTS_GRID_CERT, FTS_GRID_CERT_KEY)
        except Exception as e:
            logging.warning(f"Couldn't initialize the FTS client: {e}")
