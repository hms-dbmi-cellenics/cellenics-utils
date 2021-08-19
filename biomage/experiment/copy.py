import os
import sys

import click

from ..utils.config import get_config
from ..utils.constants import DEFAULT_SANDBOX, PRODUCTION, STAGING
from ..utils.data import copy_experiments_to


@click.command()
@click.option(
    "-e",
    "--experiment_id",
    required=True,
    show_default=True,
    help="Experiment ID to be copied.",
)
@click.option(
    "-p",
    "--prefix",
    required=False,
    default=os.getenv("BIOMAGE_EMAIL"),
    show_default=True,
    help="Prefix added to copied experiment to avoid ID clashes with other copies.",
)
@click.option(
    "-i",
    "--input_env",
    required=False,
    default=PRODUCTION,
    show_default=True,
    help="Input environment to copy the data from.",
)
@click.option(
    "-o",
    "--output_env",
    required=False,
    default=STAGING,
    show_default=True,
    help="Output environment to copy the data to.",
)
@click.option(
    "--update_hash",
    required=False,
    default=True,
    show_default=True,
    help="When adding prefixes the experiment parameters' hash will change "
    "so the pipeline will need to be run again. Set to true to update experiment"
    " hash so that the pipeline does not need to run again.",
)
def copy(experiment_id, prefix, input_env, output_env, update_hash):
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
        prefix=prefix,
        config=config,
        origin=input_env,
        destination=output_env,
        update_hash=update_hash,
    )
