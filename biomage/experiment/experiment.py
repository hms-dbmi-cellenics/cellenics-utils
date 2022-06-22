import click

from .download import download


@click.group()
def experiment():
    """
    Manage Cellenics experiment data and settings.
    """
    pass


experiment.add_command(download)
