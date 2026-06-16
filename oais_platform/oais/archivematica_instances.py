import random

from oais_platform.oais.models import Archive, Step
from oais_platform.settings import AM_INSTANCES


class ArchivematicaInstances:

    @staticmethod
    def assign(step: Step):
        am_instance_config = random.choice(AM_INSTANCES)
        step.set_input_data_field(
            "archivematica_instance", am_instance_config["AM_INSTANCE"]
        )
        return am_instance_config

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
