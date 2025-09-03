import gfal2
from django.core.management.base import BaseCommand

from oais_platform.settings import CTA_BASE_PATH


class Command(BaseCommand):
    help = "A description of my test script."

    def add_arguments(self, parser):
        parser.add_argument(
            "path",
            nargs="?",
            type=str,
            default=CTA_BASE_PATH,
            help="Path to the directory to be listed",
        )

    def _list_gfal2_directory(self, uri):
        try:
            ctx = gfal2.creat_context()
            entries = ctx.listdir(uri)
            return entries
        except Exception as e:
            self.stdout.write(self.style.ERROR((f"Error accessing directory: {e}")))
            return []

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Script started successfully!"))

        path = options["path"]
        self.stdout.write(path)
        file_list = self._list_gfal2_directory(path)

        if file_list:
            self.stdout.write(f"Contents of {path}:")
            for item in file_list:
                self.stdout.write(f"- {item}")
        else:
            self.stdout.write(
                self.style.ERROR(("No files found or an error occurred."))
            )

        self.stdout.write(self.style.SUCCESS("Script finished."))
