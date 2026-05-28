import requests
from django.core.management.base import BaseCommand

from oais_platform.oais.models import Collection


class Command(BaseCommand):
    help = "A command-line tool to reprocess a collection in a different instance."

    def add_arguments(self, parser):
        parser.add_argument(
            "env",
            type=str,
            help="Target environment to reprocess the collection (dev or qa)",
        )
        parser.add_argument(
            "collection_id", type=int, help="ID of the collection to reprocess"
        )
        parser.add_argument(
            "--token",
            type=str,
            help="Token for authentication with the target environment",
        )

    def handle(self, *args, **options):
        env = options["env"]
        collection_id = options["collection_id"]
        token = options.get("token")
        if env not in ["dev", "qa"]:
            self.stderr.write(
                self.style.ERROR(
                    "Invalid environment. Please choose either 'dev' or 'qa'."
                )
            )
            return

        if not token:
            self.stderr.write(
                self.style.ERROR(
                    "Access token not provided. Please provide a token using the --token argument."
                )
            )
            return

        try:
            collection = Collection.objects.get(id=collection_id)
            if not collection.archives.exists():
                self.stderr.write(
                    self.style.ERROR(
                        f"Collection with ID {collection_id} has no associated archives."
                    )
                )
                return
            if collection.archives.count() > 1000:
                self.stderr.write(
                    self.style.ERROR(
                        f"Collection with ID {collection_id} has more than 1000 associated archives."
                    )
                )
                return
            archive_dicts = list(collection.archives.values("source", "recid"))
        except Collection.DoesNotExist:
            self.stderr.write(
                self.style.ERROR(f"Collection with ID {collection_id} does not exist.")
            )
            return

        if env == "dev":
            instance = "https://preserve-dev.web.cern.ch/"
        elif env == "qa":
            instance = "https://preserve-qa.web.cern.ch/"

        self.stdout.write(
            self.style.SUCCESS(
                f"Reprocessing collection with ID {collection_id} in {env} environment..."
            )
        )
        try:
            res = requests.post(
                f"{instance}api/archives/harvest-recids/",
                json={"records": archive_dicts},
                headers={"Authorization": f"Bearer {token}"},
            )
            if res.status_code == 200:
                new_collection_id = res.json().get("collection_id")
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Successfully reprocessed collection with ID {collection_id} in {env} environment. New collection ID: {new_collection_id}"
                    )
                )
            else:
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed to reprocess collection with ID {collection_id} in {env} environment. Status code: {res.status_code}, Response: {res.text}"
                    )
                )
        except Exception as e:
            self.stderr.write(
                self.style.ERROR(
                    f"An error occurred while reprocessing collection with ID {collection_id} in {env} environment. Error: {str(e)}"
                )
            )
