import boto3
import click

from ..utils.constants import PRODUCTION
from .utils import add_user_to_rbac


@click.command()
@click.option(
    "-i",
    "--experiment_id",
    required=True,
    show_default=True,
    help="Experiment ID to be copied.",
)
@click.option(
    "-u",
    "--user_id",
    required=True,
    show_default=True,
    help="User to add to the experiment..",
)
@click.option(
    "-e",
    "--environment",
    required=False,
    default=PRODUCTION,
    show_default=True,
    help="Environment where the experiment is located.",
)
def add_permissions(experiment_id, user_id, environment):
    """
    Add a user to the permissions of the experiment in the environment.
    """

    table = f"experiments-{environment}"

    dynamodb = boto3.client("dynamodb")
    item = dynamodb.get_item(
        TableName=table, Key={"experimentId": {"S": experiment_id}}
    ).get("Item")
    add_user_to_rbac(user_name=user_id, cfg=item)
    dynamodb.put_item(TableName=table, Item=item)
