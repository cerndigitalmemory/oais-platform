import logging
import unittest
from unittest.mock import Mock, patch

from browse_tape import main
from click.testing import CliRunner

logging.disable(logging.CRITICAL)


class TestBrowseTapeScript(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch("browse_tape.gfal2")
    def test_browse_tape_with_path(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = ["file1.txt", "file2.csv"]
        mock_gfal2.creat_context.return_value = mock_ctx

        test_path = "/custom/path"
        result = self.runner.invoke(main, test_path)

        mock_ctx.listdir.assert_called_with(test_path)
        self.assertIn(test_path, result.output)
        self.assertIn("file1.txt", result.output)
        self.assertIn("file2.csv", result.output)

    @patch("browse_tape.gfal2")
    def test_browse_tape_error(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.side_effect = Exception("Permission denied")
        mock_gfal2.creat_context.return_value = mock_ctx

        test_path = "/custom/path"
        result = self.runner.invoke(main, test_path)

        self.assertIn("No files found or an error occurred.", result.output)

    @patch("browse_tape.gfal2")
    def test_browse_tape_empty_directory(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = []
        mock_gfal2.creat_context.return_value = mock_ctx

        test_path = "/empty/path"
        result = self.runner.invoke(main, test_path)

        self.assertIn(test_path, result.output)
        self.assertIn("No files found or an error occurred.", result.output)

    @patch("browse_tape.gfal2")
    def test_browse_tape_no_path(self, mock_gfal2):
        mock_ctx = Mock()
        mock_gfal2.creat_context.return_value = mock_ctx
        result = self.runner.invoke(main)
        self.assertIn("Error: Missing argument 'PATH'.", result.output)


if __name__ == "__main__":
    unittest.main()
