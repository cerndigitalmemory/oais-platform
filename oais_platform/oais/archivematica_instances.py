from oais_platform.oais.models import ArchivematicaInstance


class ArchivematicaInstances:

    @staticmethod
    def get_instances():
        return list(ArchivematicaInstance.objects.filter(enabled=True))

    @staticmethod
    def get_instance(archivematica_instance):
        try:
            instance = ArchivematicaInstance.objects.get(
                name=archivematica_instance, enabled=True
            )
        except ArchivematicaInstance.DoesNotExist:
            return None
        return instance
