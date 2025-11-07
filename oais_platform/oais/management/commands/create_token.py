from django.contrib.auth.models import User
from django.core.management.base import BaseCommand

from oais_platform.oais.models import PersonalAccessToken


class Command(BaseCommand):
    help = "Create Personal Access Token for a superuser"

    def add_arguments(self, parser):
        parser.add_argument("name", type=str, help="Name of the token")
        parser.add_argument("username", type=str, help="Username to create tokens for")
        parser.add_argument(
            "--force",
            action="store_true",
            help="Skip superuser check (use with caution)",
        )

    def get_tokens_for_user(self, user, name):
        if not user.is_active:
            raise Exception("User is not active")

        if PersonalAccessToken.objects.filter(user=user, name=name).exists():
            raise Exception("Token with that name already exists")

        token = PersonalAccessToken.generate_token()
        PersonalAccessToken.objects.create(user=user, name=name, token=token)

        return token

    def handle(self, *args, **options):
        name = options["name"]
        username = options["username"]
        force = options["force"]

        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'User "{username}" not found'))
            return

        # Check if user is superuser (unless forced)
        if not force and not user.is_superuser:
            self.stdout.write(
                self.style.ERROR("User must be a superuser. Use --force to override.")
            )
            return

        try:
            token = self.get_tokens_for_user(user, name)

            self.stdout.write(
                self.style.SUCCESS(f"Tokens created for user: {username}")
            )
            self.stdout.write(f"Personal Access Token: {token}")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error creating tokens: {str(e)}"))
