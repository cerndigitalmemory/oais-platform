from django.contrib.auth.models import User
from django.core.management.base import BaseCommand

from oais_platform.oais.models import Archive, Collection


class Command(BaseCommand):
    help = "A command-line tool to create collections for each source in the Archive model."

    def handle(self, *args, **options):
        sources = Archive.objects.values_list("source", flat=True).distinct()
        system_user = User.objects.filter(profile__system=True).first()

        if not system_user:
            self.stdout.write(
                self.style.ERROR(
                    "No system user found. Please create a user with profile.system=True."
                )
            )
            return

        for source in sources:
            collection, created = Collection.objects.get_or_create(
                title=Collection.get_source_collection_title(source),
                internal=True,
                creator=system_user,
                defaults={
                    "description": Collection.get_source_collection_description(source),
                },
            )

            if created:
                self.stdout.write(
                    self.style.SUCCESS(f"Created collection for source: {source}")
                )

            archives = Archive.objects.filter(source=source)
            collection.archives.set(archives)
