from oais_platform.oais.models import ArchivematicaInstance


class ArchivematicaInstances:

    @staticmethod
    def get_instance_configs():
        instances = ArchivematicaInstance.objects.filter(enabled=True)
        return [instance.as_config() for instance in instances]

    @staticmethod
    def get_instance_config(archivematica_instance):
        try:
            instance = ArchivematicaInstance.objects.get(
                name=archivematica_instance, enabled=True
            )
        except ArchivematicaInstance.DoesNotExist:
            return None
        return instance.as_config()
