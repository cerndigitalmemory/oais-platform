from django.contrib.auth.models import User
from django.core.management.base import BaseCommand
from django.db import transaction

from oais_platform.oais.models import Archive, Collection


class Command(BaseCommand):
    help = "A command-line tool to create collections for each source in the Archive model."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Number of items to process per batch (default: 100)",
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]

        sources = (
            Archive.objects.order_by("source")
            .values_list("source", flat=True)
            .distinct()
        )
        system_user = User.objects.filter(profile__system=True).first()

        if not system_user:
            self.stdout.write(
                self.style.ERROR(
                    "No system user found. Please create a user with profile.system=True."
                )
            )
            return

        for source in sources:
            self.stdout.write(f"Processing source: {source}")

            with transaction.atomic():
                collection, created = Collection.objects.get_or_create(
                    title=Collection.get_source_collection_title(source),
                    internal=True,
                    creator=system_user,
                    defaults={
                        "description": Collection.get_source_collection_description(
                            source
                        ),
                    },
                )

                if created:
                    self.stdout.write(
                        self.style.SUCCESS(f"Created collection for source: {source}")
                    )

                archive_ids = Archive.objects.filter(source=source).values_list(
                    "id", flat=True
                )
                total = len(archive_ids)

                for i in range(0, total, batch_size):
                    batch = archive_ids[i : i + batch_size]
                    with transaction.atomic():
                        collection.archives.add(*batch)

                    self.stdout.write(
                        f"Processed {min(i + batch_size, total)}/{total} archives"
                    )

                self.stdout.write(
                    self.style.SUCCESS(f"Completed {source}: {total} archives added")
                )
