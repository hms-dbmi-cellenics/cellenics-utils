import sys
from subprocess import run as sub_run

import boto3
import click

from ..utils.constants import DEFAULT_AWS_PROFILE, STAGING

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
@click.option(
    "-s",
    "--sandbox_id",
    required=False,
    default="default",
    show_default=True,
    help="Default sandbox id.",
)
@click.option(
    "-p",
    "--aws_profile",
    required=False,
    default=DEFAULT_AWS_PROFILE,
    show_default=True,
    help="The name of the profile stored in ~/.aws/credentials to use.",
)
def token(input_env, user, region, sandbox_id, aws_profile):
    """
    Generates a temporary token that can be used to login to the database (through the ssh tunnel).\n

    Examples.:\n
        cellenics rds token\n
        cellenics rds token -i staging
    """
    password = None

    db_port = 5432

    session = boto3.Session(profile_name=aws_profile)
    rds_client = session.client("rds")

    remote_endpoint = get_rds_endpoint(input_env, sandbox_id, rds_client, ENDPOINT_TYPE)

    print(f"Generating temporary token for {input_env}", file=sys.stderr)
    password = rds_client.generate_db_auth_token(remote_endpoint, db_port, user, region)

    print(f"User: {user}")
    print(f"Password: {password}")


def get_rds_endpoint(input_env, sandbox_id, rds_client, endpoint_type):
    response = rds_client.describe_db_cluster_endpoints(
        DBClusterIdentifier=f"aurora-cluster-{input_env}-{sandbox_id}",
        Filters=[
            {"Name": "db-cluster-endpoint-type", "Values": [endpoint_type]},
        ],
    )

    return response["DBClusterEndpoints"][0]["Endpoint"]
