import random

from oais_platform.oais.models import Step
from oais_platform.settings import AM_INSTANCES


class ArchivematicaInstances:

    @staticmethod
    def get_instance_config(archivematica_instance):
        return next(
            (
                am_instance_config
                for am_instance_config in AM_INSTANCES
                if am_instance_config["AM_INSTANCE"] == archivematica_instance
            ),
            None,
        )
