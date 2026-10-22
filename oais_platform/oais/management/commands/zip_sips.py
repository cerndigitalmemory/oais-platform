# This script zips existing unzipped SIPs and updates the corresponding
# step's artifact path and the archive's path and size.
# Run the script via python manage.py zip_sips
import os
import time
from datetime import timedelta

from django.core.management.base import BaseCommand

from oais_platform.oais.enums import COMPLETED_STATUSES
from oais_platform.oais.models import Archive, Step
from oais_platform.oais.tasks.utils import zip_sip_folder


class Command(BaseCommand):
    help = (
        "Zips SIPs that are still stored as a directory and updates the "
        "producing step's artifact path and the archive's path and size. "
        "Already zipped SIPs are skipped, so the script can be interrupted "
        "and re-run to continue where it stopped."
    )

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting script..."))

        statistics = {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
        }

        self.stdout.write("Looking for SIPs that are still stored as a directory...")

        steps = list(
            Step.objects.filter(
                step_type__has_sip=True,
                status__in=COMPLETED_STATUSES,
                output_data_json__artifact__artifact_name="SIP",
            )
            .exclude(output_data_json__artifact__artifact_path__endswith=".zip")
            .order_by("pk")
            .values_list("pk", "output_data_json", "archive_id")
        )
        total = len(steps)
        self.stdout.write(
            self.style.SUCCESS(f"Found {total} SIPs stored as a directory.")
        )

        started_at = time.monotonic()

        try:
            for position, (step_id, output_data, archive_id) in enumerate(
                steps, start=1
            ):
                prefix = f"[{position}/{total}]"
                current_path = (
                    (output_data or {}).get("artifact", {}).get("artifact_path")
                )

                if not current_path or not os.path.exists(current_path):
                    self.stdout.write(
                        self.style.WARNING(
                            f"{prefix} Skipping step {step_id}: "
                            f"Path does not exist ({current_path})"
                        )
                    )
                    continue

                if not os.path.isdir(current_path):
                    self.stdout.write(
                        f"{prefix} Skipping step {step_id}: SIP is already zipped"
                    )
                    continue

                try:
                    statistics["attempted"] += 1
                    zip_path = zip_sip_folder(current_path)

                    archive = Archive.objects.get(pk=archive_id)
                    if archive.path_to_sip == current_path:
                        archive.set_path(zip_path)
                        archive.update_sip_size()

                    self.stdout.write(
                        self.style.SUCCESS(
                            f"{prefix} Successfully zipped SIP for step {step_id}"
                        )
                    )
                    statistics["succeeded"] += 1
                except Exception as err:
                    self.stderr.write(
                        self.style.ERROR(
                            f"{prefix} Error zipping step {step_id}: {err}"
                        )
                    )
                    statistics["failed"] += 1
        except KeyboardInterrupt:
            self.stdout.write(
                self.style.WARNING(
                    f"\nInterrupted: {total - position} of {total} steps were not "
                    "processed. Re-run the same command to continue where this "
                    "run stopped."
                )
            )

        style = (
            self.style.ERROR
            if statistics["failed"] and statistics["failed"] == statistics["attempted"]
            else self.style.WARNING if statistics["failed"] > 0 else self.style.SUCCESS
        )

        elapsed = time.monotonic() - started_at
        summary = (
            f"Script completed in {timedelta(seconds=int(elapsed))}. "
            f"Out of {statistics['attempted']} steps with an unzipped SIP, "
            f"{statistics['succeeded']} were successfully zipped and "
            f"{statistics['failed']} failed."
        )
        if statistics["succeeded"]:
            average = timedelta(seconds=int(elapsed / statistics["succeeded"]))
            summary += f" On average {average} per zipped SIP."
        self.stdout.write(style(summary))
