from unittest.mock import patch

from django.db.models.deletion import ProtectedError
from django.test import TestCase

from oais_platform.oais.models import (
    Archive,
    ArchivematicaInstance,
    Status,
    Step,
    StepName,
)
from oais_platform.oais.tasks.archivematica import get_am_client
from oais_platform.oais.tests.am_utils import (
    AM_INSTANCES,
    create_archivematica_instance,
)


class ArchivematicaInstanceTests(TestCase):
    def test_configuration_is_stored_in_database(self):
        instance = create_archivematica_instance()

        stored_instance = ArchivematicaInstance.objects.get(
            name=instance.name, enabled=True
        )

        self.assertEqual(stored_instance, instance)
        self.assertEqual(
            list(ArchivematicaInstance.objects.filter(enabled=True)), [instance]
        )
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

        self.assertFalse(
            ArchivematicaInstance.objects.filter(
                name=instance.name, enabled=True
            ).exists()
        )
        self.assertFalse(ArchivematicaInstance.objects.filter(enabled=True).exists())

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

    def test_failed_archive_step_increments_instance_failure_count(self):
        instance = create_archivematica_instance()
        archive = Archive.objects.create(recid="1", source="test")
        step = Step.objects.create(
            archive=archive,
            step_name=StepName.ARCHIVE,
            input_data_json={"archivematica_instance": instance.name},
        )

        step.set_status(Status.FAILED)

        instance.refresh_from_db()
        step.step_type.refresh_from_db()
        self.assertEqual(instance.failed_count, 1)
        self.assertEqual(step.step_type.failed_count, 0)

    def test_failed_archive_step_uses_archive_instance_as_fallback(self):
        instance = create_archivematica_instance()
        archive = Archive.objects.create(
            archivematica_instance=instance,
            recid="1",
            source="test",
        )
        step = Step.objects.create(
            archive=archive,
            step_name=StepName.ARCHIVE,
        )

        step.set_status(Status.FAILED)

        instance.refresh_from_db()
        self.assertEqual(instance.failed_count, 1)

    def test_non_archive_failure_increments_step_type_failure_count(self):
        instance = create_archivematica_instance()
        archive = Archive.objects.create(recid="1", source="test")
        step = Step.objects.create(
            archive=archive,
            step_name=StepName.HARVEST,
        )

        step.set_status(Status.FAILED)

        instance.refresh_from_db()
        step.step_type.refresh_from_db()
        self.assertEqual(instance.failed_count, 0)
        self.assertEqual(step.step_type.failed_count, 1)

    def test_instance_is_disabled_at_archive_step_failure_limit(self):
        instance = create_archivematica_instance()
        archive = Archive.objects.create(recid="1", source="test")
        step = Step.objects.create(
            archive=archive,
            step_name=StepName.ARCHIVE,
            input_data_json={"archivematica_instance": instance.name},
        )
        step.step_type.failed_blocking_limit = 1
        step.step_type.save()

        step.set_status(Status.FAILED)

        instance.refresh_from_db()
        step.step_type.refresh_from_db()
        self.assertEqual(instance.failed_count, 1)
        self.assertFalse(instance.enabled)
        self.assertFalse(step.step_type.enabled)

    def test_archive_step_stays_enabled_while_an_instance_is_enabled(self):
        first_instance = create_archivematica_instance()
        second_instance = create_archivematica_instance(
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM2"}
        )
        archive = Archive.objects.create(recid="1", source="test")
        step = Step.objects.create(
            archive=archive,
            step_name=StepName.ARCHIVE,
            input_data_json={"archivematica_instance": first_instance.name},
        )
        step.step_type.failed_blocking_limit = 1
        step.step_type.save()

        step.set_status(Status.FAILED)

        first_instance.refresh_from_db()
        second_instance.refresh_from_db()
        step.step_type.refresh_from_db()
        self.assertFalse(first_instance.enabled)
        self.assertTrue(second_instance.enabled)
        self.assertTrue(step.step_type.enabled)

    def test_archive_step_is_disabled_after_all_instances_are_disabled(self):
        first_instance = create_archivematica_instance()
        second_instance = create_archivematica_instance(
            {**AM_INSTANCES[0], "AM_INSTANCE": "AM2"}
        )
        first_archive = Archive.objects.create(recid="1", source="test")
        second_archive = Archive.objects.create(recid="2", source="test")
        first_step = Step.objects.create(
            archive=first_archive,
            step_name=StepName.ARCHIVE,
            input_data_json={"archivematica_instance": first_instance.name},
        )
        second_step = Step.objects.create(
            archive=second_archive,
            step_name=StepName.ARCHIVE,
            input_data_json={"archivematica_instance": second_instance.name},
        )
        first_step.step_type.failed_blocking_limit = 1
        first_step.step_type.save()

        first_step.set_status(Status.FAILED)
        second_step.set_status(Status.FAILED)

        first_instance.refresh_from_db()
        second_instance.refresh_from_db()
        first_step.step_type.refresh_from_db()
        self.assertFalse(first_instance.enabled)
        self.assertFalse(second_instance.enabled)
        self.assertFalse(first_step.step_type.enabled)

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

        client, error = get_am_client(step)
        second_client, second_error = get_am_client(step)

        instance.refresh_from_db()
        self.assertFalse(error)
        self.assertFalse(second_error)
        self.assertEqual(instance.transfer_source, "discovered-transfer-source")
        self.assertEqual(client.transfer_source, "discovered-transfer-source")
        self.assertEqual(second_client.transfer_source, "discovered-transfer-source")
        discover.assert_called_once()
