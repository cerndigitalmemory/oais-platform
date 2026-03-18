import json
import os
import shutil

from django.core.management.base import BaseCommand
from django.db import transaction
from django_celery_beat.models import PeriodicTask

from oais_platform.oais.models import (
    COMPLETED_STATUSES,
    Archive,
    Collection,
    HarvestRun,
    Resource,
    Step,
    StepType,
)
from oais_platform.settings import ENVIRONMENT


class Command(BaseCommand):
    help = "A command-line tool to clean up the database and delete associated files."

    def safe_delete(self, path):
        try:
            if path and os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                return True
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to delete {path}: {e}"))
        return False

    def handle(self, *args, **options):
        if ENVIRONMENT == "prod" or ENVIRONMENT == "production":
            self.stdout.write(
                self.style.ERROR("This script cannot be run in production.")
            )
            return

        self.stdout.write(
            self.style.WARNING(
                "This will delete ALL transient data (Archive, Step, Collection ...) from the database."
            )
        )

        confirm = input("Type 'yes' to continue: ")

        if confirm.lower() != "yes":
            self.stdout.write(self.style.ERROR("Aborted."))
            return

        self.stdout.write(self.style.SUCCESS("Starting clean up script..."))

        self.stdout.write(self.style.SUCCESS("Deleting SIPs and AIPs..."))
        step_types = StepType.objects.filter(has_sip=True) | StepType.objects.filter(
            has_aip=True
        )

        outputs = Step.objects.filter(
            step_type__in=step_types,
            status__in=COMPLETED_STATUSES,
            output_data__isnull=False,
        ).values_list("output_data", "archive_id")

        for output, archive_id in outputs:
            try:
                artifact = json.loads(output).get("artifact")
                if artifact:
                    artifact_path = artifact.get("artifact_path")
                    if artifact_path and self.safe_delete(artifact_path):
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"Deleted {artifact.get('artifact_name')} from archive {archive_id}"
                            )
                        )
            except json.JSONDecodeError:
                self.stdout.write(
                    self.style.ERROR(f"Invalid JSON in output for archive {archive_id}")
                )

        self.stdout.write(self.style.SUCCESS("Deleting database objects..."))
        with transaction.atomic():
            Archive.objects.all().delete()
            Step.objects.all().delete()
            HarvestRun.objects.all().delete()
            Collection.objects.all().delete()
            Resource.objects.all().delete()
            PeriodicTask.objects.filter(task="check_am_status").delete()

        self.stdout.write(self.style.SUCCESS("Clean up completed."))
        self.stdout.write(
            self.style.SUCCESS(
                "Make sure the clean up the file system in case there are any remaining files."
            )
        )
