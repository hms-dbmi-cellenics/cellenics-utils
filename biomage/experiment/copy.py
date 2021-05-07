import click

from utils.config import get_config
from utils.data import copy_experiments_to


@click.command()
@click.argument(
    "experiment_id",
    required=True,
)
@click.argument(
    "sandbox_id",
    required=True,
)
@click.argument(
    "origin",
    default="production",
)
@click.argument(
    "destination",
    default="staging",
)
def copy(experiment_id, sandbox_id, origin, destination):
    """
    Copy a experiment from a given env. into another.
    """

    # command expects a list
    experiments = [experiment_id]

    config = get_config()
    copy_experiments_to(experiments, sandbox_id, config)
