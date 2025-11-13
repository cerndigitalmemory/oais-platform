from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import TestCase
from django.utils import timezone
from rest_framework.exceptions import AuthenticationFailed
from rest_framework.test import APIRequestFactory

from oais_platform.oais.auth import PersonalAccessTokenAuthentication
from oais_platform.oais.models import PersonalAccessToken


class PersonalAccessTokenAuthenticationTest(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.auth = PersonalAccessTokenAuthentication()

        # Create test user
        self.user = User.objects.create_user(
            username="testuser", email="test@example.com", password="testpass"
        )

        # Create test token
        self.token_value = "test-token-123"
        self.pat = PersonalAccessToken.objects.create(
            user=self.user,
            name="Test Token",
            token=self.token_value,
            expires_at=timezone.now() + timedelta(days=30),
        )

    def test_successful_authentication(self):
        """Test successful authentication with valid token"""
        request = self.factory.get("/")
        request.META["HTTP_AUTHORIZATION"] = f"Bearer {self.token_value}"

        result = self.auth.authenticate(request)

        self.assertIsNotNone(result)
        user, token = result
        self.assertEqual(user, self.user)
        self.assertEqual(token, self.pat)

        # Verify last_used_at was updated
        self.pat.refresh_from_db()
        self.assertIsNotNone(self.pat.last_used_at)

    def test_no_authorization_header(self):
        """Test authentication returns None when no auth header"""
        request = self.factory.get("/")

        result = self.auth.authenticate(request)

        self.assertIsNone(result)

    def test_invalid_authorization_format(self):
        """Test authentication returns None for invalid auth format"""
        test_cases = [
            "InvalidFormat token123",
            "Bearer",
            "Bearer ",
            "Bearer  token123",  # Double space
            "token123",
            "Basic dGVzdA==",
            "BEARER token123",
        ]

        for auth_header in test_cases:
            with self.subTest(auth_header=auth_header):
                request = self.factory.get("/")
                request.META["HTTP_AUTHORIZATION"] = auth_header

                result = self.auth.authenticate(request)

                self.assertIsNone(result)

    def test_revoked_token(self):
        """Test authentication fails for revoked token"""
        self.pat.revoked = True
        self.pat.save()

        request = self.factory.get("/")
        request.META["HTTP_AUTHORIZATION"] = f"Bearer {self.token_value}"

        with self.assertRaises(AuthenticationFailed) as cm:
            self.auth.authenticate(request)

        self.assertEqual(str(cm.exception), "Token has been revoked")

    def test_expired_token(self):
        """Test authentication fails for expired token"""
        self.pat.expires_at = timezone.now() - timedelta(days=1)
        self.pat.save()

        request = self.factory.get("/")
        request.META["HTTP_AUTHORIZATION"] = f"Bearer {self.token_value}"

        with self.assertRaises(AuthenticationFailed) as cm:
            self.auth.authenticate(request)

        self.assertEqual(str(cm.exception), "Token has expired")

    def test_inactive_user(self):
        """Test authentication fails for inactive user"""
        self.user.is_active = False
        self.user.save()

        request = self.factory.get("/")
        request.META["HTTP_AUTHORIZATION"] = f"Bearer {self.token_value}"

        with self.assertRaises(AuthenticationFailed) as cm:
            self.auth.authenticate(request)

        self.assertEqual(str(cm.exception), "User account is disabled")

    def test_last_used_timestamp_update(self):
        """Test that last_used_at timestamp is updated on successful auth"""
        # Ensure initial last_used_at is None
        self.pat.last_used_at = None
        self.pat.save()

        request = self.factory.get("/")
        request.META["HTTP_AUTHORIZATION"] = f"Bearer {self.token_value}"

        before_auth = timezone.now()
        self.auth.authenticate(request)
        after_auth = timezone.now()

        self.pat.refresh_from_db()
        self.assertIsNotNone(self.pat.last_used_at)
        self.assertGreaterEqual(self.pat.last_used_at, before_auth)
        self.assertLessEqual(self.pat.last_used_at, after_auth)

    def test_token_hash_verification(self):
        """Test that token is properly hashed for lookup"""
        # Create token with known hash
        known_token = "known-token-value"
        expected_hash = PersonalAccessToken.hash(known_token)

        pat = PersonalAccessToken.objects.create(
            user=self.user, name="Known Token", token=known_token
        )

        request = self.factory.get("/")
        request.META["HTTP_AUTHORIZATION"] = f"Bearer {known_token}"

        result = self.auth.authenticate(request)

        self.assertIsNotNone(result)
        user, token = result
        self.assertEqual(token.token_hash, expected_hash)
