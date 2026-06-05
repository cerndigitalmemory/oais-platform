import requests
from django.core.management.base import BaseCommand

from oais_platform.oais.models import Collection
from oais_platform.settings import (
    ENVIRONMENT,
    REPROCESS_TOKEN_DEV,
    REPROCESS_TOKEN_PROD,
    REPROCESS_TOKEN_QA,
)


class Command(BaseCommand):
    help = "A command-line tool to reprocess a collection in a different instance."

    def add_arguments(self, parser):
        parser.add_argument(
            "env",
            type=str,
            choices=["local", "dev", "qa", "prod"],
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
        target_env = options["env"]
        current_env = str(ENVIRONMENT).lower()
        if target_env == "prod" and current_env not in ["prod", "production"]:
            self.stderr.write(
                self.style.ERROR(
                    "You can only reprocess to the production environment from itself."
                )
            )
            return

        collection_id = options["collection_id"]
        token = options.get("token")
        if not token:
            env_token_mapping = {
                "dev": REPROCESS_TOKEN_DEV,
                "qa": REPROCESS_TOKEN_QA,
                "prod": REPROCESS_TOKEN_PROD,
            }
            token = env_token_mapping.get(target_env)
            if not token:
                self.stderr.write(
                    self.style.ERROR(
                        f"No token provided and no default token found for {target_env} environment."
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

        env_mapping = {
            "local": "http://127.0.0.1:8000",
            "dev": "https://preserve-dev.web.cern.ch",
            "qa": "https://preserve-qa.web.cern.ch",
            "prod": "https://preserve.web.cern.ch",
        }
        instance = env_mapping[target_env]

        self.stdout.write(
            self.style.SUCCESS(
                f"Reprocessing collection with ID {collection_id} in {target_env} environment..."
            )
        )
        try:
            res = requests.post(
                f"{instance}/api/archives/harvest-recids/",
                json={"records": archive_dicts},
                headers={"Authorization": f"Bearer {token}"},
            )
            if res.status_code == 200:
                try:
                    new_collection_id = res.json().get("collection_id")
                except Exception:
                    new_collection_id = "Unknown (Invalid JSON received)"
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Successfully reprocessed collection with ID {collection_id} in {target_env} environment. New collection ID: {new_collection_id}"
                    )
                )
            else:
                self.stderr.write(
                    self.style.ERROR(
                        f"Failed to reprocess collection with ID {collection_id} in {target_env} environment. Status code: {res.status_code}, Response: {res.text}"
                    )
                )
        except Exception as e:
            self.stderr.write(
                self.style.ERROR(
                    f"An error occurred while reprocessing collection with ID {collection_id} in {target_env} environment. Error: {str(e)}"
                )
            )
