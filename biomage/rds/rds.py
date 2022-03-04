import click

from .login import login
from .tunnel import tunnel


@click.group()
def rds():
    """
    Manage Cellenics RDS databases.
    """
    pass


rds.add_command(tunnel)
rds.add_command(login)
