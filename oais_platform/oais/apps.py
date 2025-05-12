import logging

from django.apps import AppConfig

from oais_platform.settings import FTS_GRID_CERT, FTS_GRID_CERT_KEY, FTS_INSTANCE

from .fts import FTS


class OaisConfig(AppConfig):
    name = "oais_platform.oais"

    def ready(self):
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("fts3.rest.client").setLevel(logging.DEBUG)

        # Initialize FTS client
        try:
            self.fts = FTS(
                FTS_INSTANCE,
                FTS_GRID_CERT,
                FTS_GRID_CERT_KEY,
            )
        except Exception as e:
            logging.warning(f"Couldn't initialize the FTS client: {e}")
