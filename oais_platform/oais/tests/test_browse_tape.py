import stat
from io import StringIO
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.test import TestCase


class TestBrowseTapeScript(TestCase):

    @patch("oais_platform.oais.management.commands.browse_tape.gfal2")
    def test_browse_tape_with_path(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = ["file1.txt", "file2.csv"]
        mock_ctx.stat.return_value = Mock(st_size=123456, st_mode=stat.S_IFREG)
        mock_gfal2.creat_context.return_value = mock_ctx

        output = StringIO()
        test_path = "/custom/path/"
        call_command("browse_tape", test_path, stdout=output)

        self.assertIn(f"Contents of {test_path}:", output.getvalue())
        self.assertIn("- file1.txt (120 KB)", output.getvalue())
        self.assertIn("- file2.csv (120 KB)", output.getvalue())

    @patch("oais_platform.oais.management.commands.browse_tape.gfal2")
    def test_browse_tape_error(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.side_effect = Exception("Permission denied")
        mock_gfal2.creat_context.return_value = mock_ctx

        output = StringIO()
        test_path = "/custom/path/"
        call_command("browse_tape", test_path, stderr=output)

        self.assertIn("Script failed: Permission denied", output.getvalue())

    @patch("oais_platform.oais.management.commands.browse_tape.gfal2")
    def test_browse_tape_empty_directory(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = []
        mock_gfal2.creat_context.return_value = mock_ctx

        output = StringIO()
        call_command("browse_tape", "/empty/path/", stdout=output)

        self.assertIn("No files found.", output.getvalue())

    @patch("oais_platform.oais.management.commands.browse_tape.gfal2")
    def test_browse_tape_nested_directories(self, mock_gfal2):
        mock_ctx = Mock()
        mock_gfal2.creat_context.return_value = mock_ctx

        mock_stat_file = Mock(st_size=100, st_mode=stat.S_IFREG)
        mock_stat_dir = Mock(st_size=0, st_mode=stat.S_IFDIR)

        root_path = "/custom/path/"

        mock_ctx.stat.side_effect = lambda path: {
            f"{root_path}file_a.txt": mock_stat_file,
            f"{root_path}subdir": mock_stat_dir,
            f"{root_path}subdir/nested_file_b.dat": mock_stat_file,
        }.get(path.rstrip("/"))

        mock_ctx.listdir.side_effect = lambda path: {
            root_path: ["file_a.txt", "subdir"],
            f"{root_path}subdir/": ["nested_file_b.dat"],
        }.get(path, [])

        output = StringIO()
        call_command("browse_tape", root_path, stdout=output)

        output = output.getvalue()
        self.assertIn("- file_a.txt (100 bytes)", output)
        self.assertIn("- subdir (100 bytes)", output)
        self.assertIn("    - nested_file_b.dat (100 bytes)", output)
