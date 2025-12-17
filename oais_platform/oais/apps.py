import logging
import sys

from django.apps import AppConfig

from oais_platform.settings import FTS_GRID_CERT, FTS_GRID_CERT_KEY, FTS_INSTANCE

from .fts import FTS


class OaisConfig(AppConfig):
    name = "oais_platform.oais"

    def ready(self):
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        logging.getLogger("fts3.rest.client").setLevel(logging.DEBUG)

        # Skip initialization for some management command
        skip_commands = ["migrate", "makemigrations", "collectstatic", "create_token"]
        if len(sys.argv) > 1 and sys.argv[1] in skip_commands:
            return

        # Initialize FTS client
        try:
            self.fts = FTS(
                FTS_INSTANCE,
                FTS_GRID_CERT,
                FTS_GRID_CERT_KEY,
            )
        except Exception as e:
            logging.warning(f"Couldn't initialize the FTS client: {e}")

    def get_fts_client(self):
        if not hasattr(self, "fts"):
            raise RuntimeError("FTS client is not configured.")
        return self.fts
