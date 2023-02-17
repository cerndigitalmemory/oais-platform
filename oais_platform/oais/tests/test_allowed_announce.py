from django.contrib.auth.models import User
from oais_platform.oais.views import check_allowed_path
from rest_framework.test import APITestCase


class AnnouncePathAllowedTests(APITestCase):
    def test_paths(self):
        self.assertFalse(check_allowed_path("/a/path/123", "standarduser"))
        self.assertFalse(check_allowed_path("/eos/users-a/asvas", "standarduser"))
        self.assertFalse(check_allowed_path("a/dfa/eos", "standarduser"))
        self.assertFalse(check_allowed_path("eos/a/b/c/d/e/", "standarduser"))
        self.assertFalse(check_allowed_path("/eos/home-s/standarduser", "standarduser"))

        self.assertTrue(
            check_allowed_path("/eos/home-s/standarduser/a", "standarduser")
        )
