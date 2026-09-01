# This script zips existing unzipped SIPs and updates the corresponding
# step's artifact path and the archive's path and size.
# Run the script via python manage.py zip_sips
import os

from django.core.management.base import BaseCommand

from oais_platform.oais.enums import COMPLETED_STATUSES
from oais_platform.oais.models import Step
from oais_platform.oais.tasks.utils import create_path_artifact, zip_sip_folder
from oais_platform.settings import SIP_STORE_BASEPATH


class Command(BaseCommand):
    help = (
        "Zips SIPs that are still stored as a directory and updates the "
        "producing step's artifact path and the archive's path and size."
    )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting script..."))

        statistics = {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
        }

        steps = Step.objects.filter(
            step_type__has_sip=True, status__in=COMPLETED_STATUSES
        )

        for step in steps:
            artifact = (step.output_data_json or {}).get("artifact")
            if not artifact or artifact.get("artifact_name") != "SIP":
                continue

            current_path = artifact.get("artifact_path")

            if not current_path or not os.path.exists(current_path):
                self.stdout.write(
                    self.style.WARNING(
                        f"Skipping step {step.id}: Path does not exist ({current_path})"
                    )
                )
                continue

            if not os.path.isdir(current_path):
                self.stdout.write(f"Skipping step {step.id}: SIP is already zipped")
                continue

            try:
                statistics["attempted"] += 1
                zip_path = zip_sip_folder(current_path)

                artifact = create_path_artifact(
                    "SIP",
                    os.path.join(SIP_STORE_BASEPATH, zip_path),
                    zip_path,
                )
                step.set_output_data_field("artifact", artifact)

                archive = step.archive
                if archive.path_to_sip == current_path:
                    archive.set_path(zip_path)
                    archive.update_sip_size()

                self.stdout.write(
                    self.style.SUCCESS(f"Successfully zipped SIP for step {step.id}")
                )
                statistics["succeeded"] += 1
            except Exception as e:
                self.stderr.write(
                    self.style.ERROR(f"Error zipping step {step.id}: {str(e)}")
                )
                statistics["failed"] += 1

        style = (
            self.style.ERROR
            if statistics["failed"] == statistics["attempted"]
            else self.style.WARNING if statistics["failed"] > 0 else self.style.SUCCESS
        )

        self.stdout.write(
            style(
                f"Script completed. Out of {statistics['attempted']} steps with an unzipped SIP, "
                f"{statistics['succeeded']} were successfully zipped and {statistics['failed']} failed."
            )
        )
