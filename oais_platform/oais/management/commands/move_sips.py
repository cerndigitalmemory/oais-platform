# This script is used to move the SIP folders to the new directory structure and to update the paths in the database
# Run the script via python manage.py move_sips
import os
import shutil

from django.core.management.base import BaseCommand

from oais_platform.oais.models import Archive
from oais_platform.oais.tasks.utils import generate_directory_structure
from oais_platform.settings import BIC_UPLOAD_PATH


class Command(BaseCommand):
    help = "Moves SIPs to a new directory structure"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting script..."))

        archives = Archive.objects.filter(path_to_sip__isnull=False).exclude(
            path_to_sip=""
        )

        for archive in archives:
            current_path = archive.path_to_sip

            if not current_path or not os.path.exists(current_path):
                self.stdout.write(
                    self.style.WARNING(
                        f"Skipping archive {archive.id}: Path does not exist ({current_path})"
                    )
                )
                continue

            folder_name = os.path.basename(current_path)
            new_structure = generate_directory_structure(BIC_UPLOAD_PATH, archive)
            new_path = os.path.join(new_structure, folder_name)

            if current_path == new_path:
                self.stdout.write(
                    f"Skipping archive {archive.id}: Already in correct folder"
                )
                continue

            try:
                os.makedirs(new_structure, exist_ok=True)
                shutil.move(current_path, new_path)

                archive.path_to_sip = new_path
                archive.save()

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Successfully moved SIP for archive {archive.id}"
                    )
                )
            except Exception as e:
                self.stderr.write(
                    self.style.ERROR(f"Error moving archive {archive.id}: {str(e)}")
                )

        self.stdout.write(self.style.SUCCESS("Script completed"))
