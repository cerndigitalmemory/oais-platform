import os
import shutil

from django.core.management.base import BaseCommand
from django.db import transaction
from django_celery_beat.models import PeriodicTask

from oais_platform.oais.models import Archive, Collection, HarvestRun, Resource
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
        archives = Archive.objects.only("id", "path_to_sip", "path_to_aip")

        ids_to_delete = []

        self.stdout.write(self.style.SUCCESS("Deleting SIPs and AIPs..."))
        for a in archives.iterator():
            self.stdout.write(f"Archive {a.id}")
            if self.safe_delete(a.path_to_sip):
                self.stdout.write(self.style.SUCCESS("Deleted SIP"))
            if self.safe_delete(a.path_to_aip):
                self.stdout.write(self.style.SUCCESS("Deleted AIP"))
            ids_to_delete.append(a.id)

        self.stdout.write(self.style.SUCCESS("Deleting database objects..."))
        with transaction.atomic():
            Archive.objects.filter(id__in=ids_to_delete).delete()
            HarvestRun.objects.all().delete()
            Collection.objects.all().delete()
            Resource.objects.all().delete()
            PeriodicTask.objects.filter(task="check_am_status").all().delete()

        self.stdout.write(self.style.SUCCESS("Clean up completed successfully."))
