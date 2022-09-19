import click

from .download import download
from .upload import upload


@click.group()
def experiment():
    """
    Manage Cellenics experiment data and settings.
    """
    pass


experiment.add_command(download)
experiment.add_command(upload)
