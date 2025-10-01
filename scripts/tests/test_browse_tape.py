import stat
import unittest
from unittest.mock import Mock, patch

from browse_tape import main
from click.testing import CliRunner


class TestBrowseTapeScript(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    @patch("browse_tape.gfal2")
    def test_browse_tape_with_path(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = ["file1.txt", "file2.csv"]
        mock_ctx.stat.return_value = Mock(st_size=123456, st_mode=stat.S_IFREG)
        mock_gfal2.creat_context.return_value = mock_ctx

        test_path = "/custom/path/"
        result = self.runner.invoke(main, test_path)

        mock_ctx.listdir.assert_called_with(test_path)
        self.assertIn(f"Contents of {test_path}:", result.output)
        self.assertIn("- file1.txt (120 KB)", result.output)
        self.assertIn("- file2.csv (120 KB)", result.output)

    @patch("browse_tape.gfal2")
    def test_browse_tape_error(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.side_effect = Exception("Permission denied")
        mock_gfal2.creat_context.return_value = mock_ctx

        test_path = "/custom/path/"
        result = self.runner.invoke(main, test_path)

        self.assertIn("An error occurred: Permission denied", result.output)

    @patch("browse_tape.gfal2")
    def test_browse_tape_empty_directory(self, mock_gfal2):
        mock_ctx = Mock()
        mock_ctx.listdir.return_value = []
        mock_gfal2.creat_context.return_value = mock_ctx

        test_path = "/empty/path/"
        result = self.runner.invoke(main, test_path)

        self.assertIn("No files found.", result.output)

    @patch("browse_tape.gfal2")
    def test_browse_tape_no_path(self, mock_gfal2):
        mock_ctx = Mock()
        mock_gfal2.creat_context.return_value = mock_ctx
        result = self.runner.invoke(main)
        self.assertIn("Error: Missing argument 'PATH'.", result.output)

    @patch("browse_tape.gfal2")
    def test_browse_tape_nested_directories(self, mock_gfal2):
        mock_ctx = Mock()
        mock_gfal2.creat_context.return_value = mock_ctx
        mock_stat_file = Mock(st_size=100, st_mode=stat.S_IFREG)
        mock_stat_dir = Mock(st_size=0, st_mode=stat.S_IFDIR)

        root_path = "/custom/path/"
        nested_path = f"{root_path}subdir/"

        mock_ctx.stat.side_effect = lambda path: {
            f"{root_path}file_a.txt": mock_stat_file,
            f"{root_path}subdir": mock_stat_dir,
            f"{nested_path}nested_file_b.dat": mock_stat_file,
        }.get(path)

        mock_ctx.listdir.side_effect = lambda path: {
            root_path: ["file_a.txt", "subdir"],
            nested_path: ["nested_file_b.dat"],
        }.get(path, [])

        result = self.runner.invoke(main, root_path)

        mock_ctx.listdir.assert_any_call(root_path)
        mock_ctx.listdir.assert_any_call(nested_path)
        mock_ctx.stat.assert_any_call(f"{root_path}subdir")
        mock_ctx.stat.assert_any_call(f"{nested_path}nested_file_b.dat")

        self.assertIn(f"Contents of {root_path}:", result.output)
        self.assertIn("- file_a.txt (100 bytes)", result.output)
        self.assertIn("- subdir (100 bytes)", result.output)
        self.assertIn("    - nested_file_b.dat (100 bytes)", result.output)
