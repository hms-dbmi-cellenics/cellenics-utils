import click

from .download import download
from .info import info
from .upload import upload


@click.group()
def experiment():
    """
    Manage Cellenics experiment data and settings.
    """
    pass


experiment.add_command(download)
experiment.add_command(upload)
experiment.add_command(info)
