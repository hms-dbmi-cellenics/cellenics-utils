import os
import sys

import click

from utils.config import get_config
from utils.constants import DEFAULT_SANDBOX, PRODUCTION, STAGING
from utils.data import copy_experiments_to


@click.command()
@click.option(
    "-e", "--experiment_id", required=True, help="Experiment ID to be copied."
)
@click.option(
    "-s",
    "--sandbox_id",
    required=True,
    default=DEFAULT_SANDBOX,
    help="Sandbox ID in the destination environment to copy the data to.",
)
@click.option(
    "-i",
    "--input_env",
    required=False,
    default=PRODUCTION,
    help="Input environment to copy the data from.",
)
@click.option(
    "-o",
    "--output_env",
    required=False,
    default=STAGING,
    help="Output environment to copy the data to.",
)
@click.option(
    "-u",
    "--username",
    required=False,
    default=False,
    help="Username for the user to be added to the copied experiment",
)
def copy(experiment_id, sandbox_id, username, input_env, output_env):
    """
    Copy an experiment from the default sandbox of the input
    environment into the sandbox_id of the output environment.
    """

    if output_env == PRODUCTION:
        click.echo(f"Cowardly refusing to copy data to {PRODUCTION} environment.")
        sys.exit(1)

    username = username if username else os.getenv("BIOMAGE_STAGING_USERNAME")

    if not username:
        click.echo("Please provide a username (-u/--user) or add BIOMAGE_STAGING_USERNAME in your ENV")
        sys.exit(1)

    # the function expects a list of ids
    experiments = [experiment_id]

    config = get_config()
    copy_experiments_to(
        experiments=experiments,
        sandbox_id=sandbox_id,
        username=username,
        config=config,
        origin=input_env,
        destination=output_env,
    )
