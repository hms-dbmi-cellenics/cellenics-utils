import sys

import click

from ..utils.config import get_config
from ..utils.constants import DEFAULT_SANDBOX, PRODUCTION, STAGING
from ..utils.data import copy_experiments_to


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
def copy(experiment_id, sandbox_id, input_env, output_env):
    """
    Copy an experiment from the default sandbox of the input environment into the
    sandbox_id of the output environment.
    """

    if output_env == PRODUCTION:
        click.echo(f"Cowardly refusing to copy data to {PRODUCTION} environment.")
        sys.exit(1)

    # the function expects a list of ids
    experiments = [experiment_id]
    config = get_config()

    copy_experiments_to(
        experiments=experiments,
        sandbox_id=sandbox_id,
        config=config,
        origin=input_env,
        destination=output_env,
    )
