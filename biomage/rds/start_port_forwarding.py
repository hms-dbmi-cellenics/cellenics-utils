import sys

import click

from ..utils.config import get_config
from ..utils.constants import STAGING


@click.command()

@click.option(
    "-i",
    "--input_env",
    required=False,
    default=STAGING,
    show_default=True,
    help="Input environment of the RDS server.",
)

@click.option(
    "-p",
    "--local_port",
    required=False,
    default=5432,
    show_default=True,
    help="Local port from which to connect.",
)

def start_port_forwarding(input_env, local_port):
    """
    Sets up a port forwarding session for the rds server in a given environment.\n

    E.g.:
    biomage rds start_port_forwarding -i staging -p 5432
    """