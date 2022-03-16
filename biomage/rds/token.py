import sys
from subprocess import run as sub_run

import boto3
import click

from ..utils.constants import STAGING

# we use writer because reader might also point to writer making it not safe
ENDPOINT_TYPE = "writer"


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
    "-u",
    "--user",
    required=False,
    default="dev_role",
    show_default=True,
    help="User to connect as (role is the same as user).",
)
@click.option(
    "-r",
    "--region",
    required=False,
    default="eu-west-1",
    show_default=True,
    help="Region the RDS server is in.",
)
def token(input_env, user, region):
    """
    Generates a temporary token that can be used to login to the database (through the ssh tunnel).\n

    Examples.:\n
        biomage rds token\n
        biomage rds token -i staging
    """
    password = None

    internal_port = 5432

    rds_client = boto3.client("rds")

    remote_endpoint = get_rds_endpoint(input_env, rds_client, ENDPOINT_TYPE)

    print(f"Generating temporary token for {input_env}", file=sys.stderr)
    password = rds_client.generate_db_auth_token(
        remote_endpoint, internal_port, user, region
    )

    print(f"User: {user}")
    print(f"Password: {password}")


def get_rds_endpoint(input_env, rds_client, endpoint_type):
    response = rds_client.describe_db_cluster_endpoints(
        DBClusterIdentifier=f"aurora-cluster-{input_env}",
        Filters=[
            {"Name": "db-cluster-endpoint-type", "Values": [endpoint_type]},
        ],
    )

    return response["DBClusterEndpoints"][0]["Endpoint"]
