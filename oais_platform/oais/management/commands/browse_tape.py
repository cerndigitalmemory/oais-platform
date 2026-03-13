# browse_tape.py
# This script is used to list files in a directory using gfal2.
# Run the script via python3 manage.py browse_tape <path>
import stat
import time

import gfal2
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "A command-line tool to list files in a directory using gfal2."

    def add_arguments(self, parser):
        parser.add_argument("path", type=str)
        parser.add_argument("--summary", action="store_true", default=False)

    def human_readable_size(
        self, size_bytes, units=["bytes", "KB", "MB", "GB", "TB", "PB", "EB"]
    ):
        """Returns a human readable string representation of bytes"""
        return (
            f"{str(size_bytes)} {units[0]}"
            if size_bytes < 1024
            else self.human_readable_size(size_bytes >> 10, units[1:])
        )

    def print_directory_contents(self, files, summary, indent=0):
        """Recursively prints the contents of the directory."""
        indent_space = "    " * indent
        if isinstance(files, dict):
            for (entry, size), subentry in files.items():
                statement = f"{indent_space}- {entry}"
                if not summary:
                    statement += f" ({self.human_readable_size(size)})"
                self.stdout.write(statement)
                self.print_directory_contents(subentry, summary, indent + 1)

    def list_gfal2_directory(self, ctx, uri, summary):
        """Lists the contents of a directory using gfal2."""
        result = {}
        try:
            self.stdout.write(f"Listing directory {uri}")
            entries = ctx.listdir(uri)
            for entry in entries:
                full_path = f"{uri.rstrip('/')}/{entry}"
                entry_stat = ctx.stat(full_path)
                size = entry_stat.st_size
                if stat.S_ISDIR(entry_stat.st_mode) and not summary:
                    try:
                        directory = self.list_gfal2_directory(
                            ctx, f"{full_path}/", summary
                        )
                        size = sum([file_size for (_, file_size) in directory.keys()])
                        result.update({(entry, size): directory})
                    except Exception:
                        result.update({(entry, size): {}})
                else:
                    result.update({(entry, size): {}})
            return result
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error accessing directory {uri}: {e}"))
            raise e

    def count_files(self, files):
        """Recursively counts the number of files and directories."""
        file_count = 0
        directory_count = 0
        for entry in files.values():
            if not entry:
                file_count += 1
            else:
                directory_count += 1
            new_files, new_dirs = self.count_files(entry)
            file_count += new_files
            directory_count += new_dirs
        return file_count, directory_count

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Starting script..."))

        path = options["path"]
        summary = options["summary"]
        start_time = time.time()

        try:
            gfal2.set_verbose(gfal2.verbose_level.warning)
            ctx = gfal2.creat_context()
            files = self.list_gfal2_directory(ctx, path, summary)
            if files:
                self.stdout.write(self.style.SUCCESS(f"\nContents of {path}:"))
                self.print_directory_contents(files, summary)
            else:
                self.stdout.write(self.style.WARNING("No files found."))

            end_time = time.time()
            duration_seconds = end_time - start_time
            if files and not summary:
                file_count, directory_count = self.count_files(files)
                records_per_second = (
                    (file_count / duration_seconds) if duration_seconds > 0 else 0
                )
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Script finished. Processing took {(duration_seconds / 60):.2f} minutes "
                        f"({records_per_second:.2f} files per second (total of {file_count} files and {directory_count} directories))"
                    )
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Script finished. Processing took {(duration_seconds / 60):.2f} minutes"
                    )
                )
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Script failed: {str(e)}"))
