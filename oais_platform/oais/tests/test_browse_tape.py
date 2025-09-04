from io import StringIO
from unittest.mock import Mock, patch

from django.core.management import call_command
from rest_framework.test import APITestCase

from oais_platform.settings import CTA_BASE_PATH


class BrowseTapeTests(APITestCase):
    @patch("oais_platform.oais.management.commands.browse_tape.gfal2")
    def test_browse_tape_with_path(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = ["file1.txt", "file2.csv"]
        mock_gfal2.creat_context.return_value = mock_ctx

        out = StringIO()
        test_path = "/custom/path"

        call_command("browse_tape", test_path, stdout=out)
        mock_ctx.listdir.assert_called_with(test_path)

        output = out.getvalue()
        self.assertIn("Script started successfully!", output)
        self.assertIn(test_path, output)
        self.assertIn("file1.txt", output)
        self.assertIn("file2.csv", output)
        self.assertIn("Script finished.", output)

    @patch("oais_platform.oais.management.commands.browse_tape.gfal2")
    def test_browse_tape_with_default_path(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = ["default_file1.txt", "default_file2.csv"]
        mock_gfal2.creat_context.return_value = mock_ctx

        out = StringIO()

        call_command("browse_tape", stdout=out)
        mock_ctx.listdir.assert_called_with(CTA_BASE_PATH)

        output = out.getvalue()
        self.assertIn("Script started successfully!", output)
        self.assertIn(CTA_BASE_PATH, output)
        self.assertIn("default_file1.txt", output)
        self.assertIn("default_file2.csv", output)
        self.assertIn("Script finished.", output)

    @patch("oais_platform.oais.management.commands.browse_tape.gfal2")
    def test_browse_tape_error(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.side_effect = Exception("Permission denied")
        mock_gfal2.creat_context.return_value = mock_ctx

        out = StringIO()

        call_command("browse_tape", stdout=out)

        output = out.getvalue()
        self.assertIn("Script started successfully!", output)
        self.assertIn("Error accessing directory: Permission denied", output)
        self.assertIn("No files found or an error occurred.", output)

    @patch("oais_platform.oais.management.commands.browse_tape.gfal2")
    def test_browse_tape_empty_directory(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = []
        mock_gfal2.creat_context.return_value = mock_ctx

        out = StringIO()

        call_command("browse_tape", "/empty/path", stdout=out)

        output = out.getvalue()
        self.assertIn("Script started successfully!", output)
        self.assertIn("No files found or an error occurred.", output)
