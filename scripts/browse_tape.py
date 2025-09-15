import logging

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
            entries = ctx.listdir(uri)
            for entry in entries:
                stat = ctx.stat(f"{uri}{entry}")
                size = stat.st_size
                if size == 0 and not summary:
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
            logging.warning(f"Error accessing directory: {e}")
            raise e

    logging.info("Script started successfully!")
    click.echo(f"Listing contents of: {path}")

    try:
        ctx = gfal2.creat_context()
        files = list_gfal2_directory(ctx, path)
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        files = None

    if files:
        click.echo(f"\nContents of {path}:")
        print_directory_contents(files)
    else:
        click.echo(click.style("No files found or an error occurred.", fg="red"))

    logging.info("Script finished.")


if __name__ == "__main__":
    main()
