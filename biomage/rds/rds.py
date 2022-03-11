import click

from .run import run
from .tunnel import tunnel


@click.group()
def rds():
    """
    Manage Cellenics RDS databases.
    """
    pass


rds.add_command(tunnel)
rds.add_command(run)
