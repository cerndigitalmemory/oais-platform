import logging

import click
import gfal2

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def list_gfal2_directory(uri):
    """
    Lists the contents of a directory using gfal2.
    """
    try:
        ctx = gfal2.creat_context()
        entries = ctx.listdir(uri)
        return entries
    except Exception as e:
        logging.error(f"Error accessing directory: {e}")
        return []


@click.command()
@click.argument(
    "path",
    type=str,
    required=True,
)
def main(path):
    """
    A command-line tool to list files in a directory using gfal2.
    """
    logging.info("Script started successfully!")

    click.echo(f"Listing contents of: {path}")

    file_list = list_gfal2_directory(path)

    if file_list:
        click.echo(f"\nContents of {path}:")
        for item in file_list:
            click.echo(f"- {item}")
    else:
        click.echo(click.style("No files found or an error occurred.", fg="red"))

    logging.info("Script finished.")


if __name__ == "__main__":
    main()
