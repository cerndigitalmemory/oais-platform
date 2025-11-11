from io import StringIO

from django.contrib.auth.models import User
from django.core.management import call_command
from django.test import TestCase

from oais_platform.oais.models import PersonalAccessToken


class CreateTokenCommandTest(TestCase):
    def setUp(self):
        self.superuser = User.objects.create_superuser(
            username="superuser", email="admin@test.com", password="pass"
        )
        self.regular_user = User.objects.create_user(
            username="user", email="user@test.com", password="pass"
        )
        self.inactive_user = User.objects.create_user(
            username="inactive",
            email="inactive@test.com",
            password="pass",
            is_active=False,
        )

    def test_create_token_for_superuser(self):
        out = StringIO()
        call_command("create_token", "test-token", "superuser", stdout=out)

        output = out.getvalue()
        self.assertIn("Tokens created for user: superuser", output)
        self.assertIn("Personal Access Token:", output)

        # Verify token was actually created
        token = PersonalAccessToken.objects.get(user=self.superuser, name="test-token")
        self.assertIsNotNone(token.token_hash)

    def test_create_token_for_regular_user_fails(self):
        out = StringIO()
        call_command("create_token", "test-token", "user", stdout=out)

        self.assertIn("User must be a superuser", out.getvalue())
        self.assertFalse(
            PersonalAccessToken.objects.filter(user=self.regular_user).exists()
        )

    def test_create_token_with_force_flag(self):
        out = StringIO()
        call_command("create_token", "test-token", "user", "--force", stdout=out)

        self.assertIn("Tokens created for user: user", out.getvalue())
        self.assertTrue(
            PersonalAccessToken.objects.filter(
                user=self.regular_user, name="test-token"
            ).exists()
        )

    def test_create_token_for_nonexistent_user(self):
        out = StringIO()
        call_command("create_token", "test-token", "nonexistent", stdout=out)

        self.assertIn('User "nonexistent" not found', out.getvalue())

    def test_create_token_for_inactive_user(self):
        out = StringIO()
        call_command("create_token", "test-token", "inactive", "--force", stdout=out)

        self.assertIn("User is not active", out.getvalue())

    def test_duplicate_token_name_fails(self):
        # Create first token
        PersonalAccessToken.objects.create(
            user=self.superuser, name="duplicate-token", token="test123"
        )

        out = StringIO()
        call_command("create_token", "duplicate-token", "superuser", stdout=out)

        self.assertIn("Token with that name already exists", out.getvalue())
