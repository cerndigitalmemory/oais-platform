import logging
import stat
import time

import click
import gfal2

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def human_readable_size(bytes, units=["bytes", "KB", "MB", "GB", "TB", "PB", "EB"]):
    """Returns a human readable string representation of bytes"""
    return (
        f"{str(bytes)} {units[0]}"
        if bytes < 1024
        else human_readable_size(bytes >> 10, units[1:])
    )


@click.command()
@click.argument(
    "path",
    type=str,
    required=True,
)
@click.option("--summary", is_flag=True, default=False)
def main(path, summary):
    """
    A command-line tool to list files in a directory using gfal2.
    """

    def print_directory_contents(files, indent=0):
        """
        Recursively prints the contents of the directory.
        """
        indent_space = "    " * indent
        if isinstance(files, dict):
            for (entry, size), subentry in files.items():
                statement = f"{indent_space}- {entry}"
                if not summary:
                    statement += f" ({human_readable_size(size)})"
                click.echo(statement)
                print_directory_contents(subentry, indent + 1)

    def list_gfal2_directory(ctx, uri):
        """
        Lists the contents of a directory using gfal2.
        """
        result = {}
        try:
            logging.info(f"Listing directory {uri}")
            entries = ctx.listdir(uri)
            for entry in entries:
                entry_stat = ctx.stat(f"{uri}{entry}")
                size = entry_stat.st_size
                if stat.S_ISDIR(entry_stat.st_mode) and not summary:
                    try:
                        directory = list_gfal2_directory(ctx, f"{uri}{entry}/")
                        size = sum([file_size for (_, file_size) in directory.keys()])
                        result.update({(entry, size): directory})
                    except Exception:
                        result.update({(entry, size): {}})
                else:
                    result.update({(entry, size): {}})

            return result
        except Exception as e:
            logging.error(f"Error accessing directory: {e}")
            raise e

    def count_files(files):
        """
        Recursively counts the number of files and directories.
        """
        file_count = 0
        directory_count = 0
        for entry in files.values():
            if not entry:
                file_count += 1
            else:
                directory_count += 1
            new_files, new_dirs = count_files(entry)
            file_count += new_files
            directory_count += new_dirs
        return file_count, directory_count

    start_time = time.time()
    logging.info("Script started successfully!")
    logging.info(f"Listing contents of: {path}\n")

    try:
        gfal2.set_verbose(gfal2.verbose_level.warning)
        ctx = gfal2.creat_context()
        files = list_gfal2_directory(ctx, path)
        if files:
            click.echo(f"\nContents of {path}:")
            print_directory_contents(files)
        else:
            click.echo(click.style("No files found.", fg="red"))
    except Exception as e:
        click.echo(click.style(f"An error occurred: {e}", fg="red"))

    end_time = time.time()
    duration_seconds = end_time - start_time
    if files and not summary:
        file_count, directory_count = count_files(files)
        records_per_second = (
            file_count / duration_seconds if duration_seconds > 0 else 0
        )
        logging.info(
            f"Processing took {(duration_seconds / 60):.2f} minutes "
            f"({records_per_second:.2f} files per second (total of {file_count} files and {directory_count} directories))"
        )
    else:
        logging.info(f"Processing took {(duration_seconds / 60):.2f} minutes")

    logging.info("Script finished.")


if __name__ == "__main__":
    main()
