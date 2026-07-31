from unittest.mock import patch

from django.db.models.deletion import ProtectedError
from django.test import TestCase

from oais_platform.oais.archivematica_instances import ArchivematicaInstances
from oais_platform.oais.models import Archive, ArchivematicaInstance, Step, StepName
from oais_platform.oais.tests.archivematica import (
    AM_INSTANCES,
    create_archivematica_instance,
)
from oais_platform.oais.tasks.archivematica import get_am_client


class ArchivematicaInstanceTests(TestCase):
    def test_configuration_is_stored_in_database(self):
        instance = create_archivematica_instance()

        stored_instance = ArchivematicaInstances.get_instance(instance.name)

        self.assertEqual(stored_instance, instance)
        self.assertEqual(ArchivematicaInstances.get_instances(), [instance])
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

        self.assertIsNone(ArchivematicaInstances.get_instance(instance.name))
        self.assertEqual(ArchivematicaInstances.get_instances(), [])

    def test_instance_names_are_unique(self):
        field = ArchivematicaInstance._meta.get_field("name")

        self.assertTrue(field.primary_key)
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

    @patch(
        "oais_platform.oais.tasks.archivematica.get_transfer_source",
        return_value="discovered-transfer-source",
    )
    def test_discovered_transfer_source_is_saved_and_reused(self, discover):
        instance = create_archivematica_instance()
        archive = Archive.objects.create(recid="1", source="test")
        step = Step.objects.create(
            archive=archive,
            step_name=StepName.ARCHIVE,
            input_data_json={"archivematica_instance": instance.name},
        )

        error, client = get_am_client(step)
        second_error, second_client = get_am_client(step)

        instance.refresh_from_db()
        self.assertFalse(error)
        self.assertFalse(second_error)
        self.assertEqual(instance.transfer_source, "discovered-transfer-source")
        self.assertEqual(client.transfer_source, "discovered-transfer-source")
        self.assertEqual(second_client.transfer_source, "discovered-transfer-source")
        discover.assert_called_once()
