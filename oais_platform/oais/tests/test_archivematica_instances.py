from django.db.models.deletion import ProtectedError
from django.test import TestCase

from oais_platform.oais.archivematica_instances import ArchivematicaInstances
from oais_platform.oais.models import Archive, ArchivematicaInstance
from oais_platform.oais.tests.archivematica import (
    AM_INSTANCES,
    create_archivematica_instance,
)


class ArchivematicaInstanceTests(TestCase):
    def test_configuration_is_stored_in_database(self):
        instance = create_archivematica_instance()

        config = ArchivematicaInstances.get_instance_config(instance.name)

        self.assertEqual(config, AM_INSTANCES[0])
        instance.refresh_from_db()
        self.assertNotEqual(instance._api_key, AM_INSTANCES[0]["AM_API_KEY"])
        self.assertNotEqual(
            instance._storage_service_api_key,
            AM_INSTANCES[0]["AM_SS_API_KEY"],
        )

    def test_disabled_instances_are_not_available(self):
        instance = create_archivematica_instance()
        instance.enabled = False
        instance.save()

        self.assertIsNone(ArchivematicaInstances.get_instance_config(instance.name))
        self.assertEqual(ArchivematicaInstances.get_instance_configs(), [])

    def test_instance_names_are_unique(self):
        field = ArchivematicaInstance._meta.get_field("name")

        self.assertTrue(field.unique)

    def test_archive_references_instance_by_name(self):
        instance = create_archivematica_instance()
        archive = Archive.objects.create(
            archivematica_instance=instance,
            recid="1",
            source="test",
        )

        archive.refresh_from_db()

        self.assertEqual(archive.archivematica_instance, instance)
        self.assertEqual(archive.archivematica_instance_id, instance.name)
        with self.assertRaises(ProtectedError):
            instance.delete()
