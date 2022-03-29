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
    "-s",
    "--sandbox_id",
    required=False,
    default="default",
    show_default=True,
    help="Default sandbox id.",
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
@click.argument("command")
def run(command, sandbox_id, input_env, user, region):
    """
    Runs the provided command in the cluster using IAM if necessary.
    Use 'psql' to start an interactive session.
    Use 'pg_dump' to get a dump of the database.\n

    Examples.:\n
        biomage rds run psql\n
        biomage rds run pg_dump > dump.sql
    """
    password = None

    internal_port = 5432

    if input_env == "development":
        password = "password"
        internal_port = 5431
    else:
        rds_client = boto3.client("rds")

        remote_endpoint = get_rds_endpoint(
            input_env, sandbox_id, rds_client, ENDPOINT_TYPE
        )

        print(
            f"Generating temporary token for {input_env}-{sandbox_id}", file=sys.stderr
        )
        password = rds_client.generate_db_auth_token(
            remote_endpoint, internal_port, user, region
        )

    print("Token generated", file=sys.stderr)

    result = sub_run(
        f'PGPASSWORD="{password}" {command} \
            --host=localhost \
            --port={internal_port} \
            --username={user} \
            --dbname=aurora_db',
        shell=True,
    )

    if result.returncode != 0:
        print(
            "\n"
            "There was an error connecting to the db. "
            'You may need to install psql, run "brew install postgresql"'
            "\n\n"
            'Or try running "biomage rds tunnel" before this command if connecting'
            "to staging/production"
        )


def get_rds_endpoint(input_env, sandbox_id, rds_client, endpoint_type):
    response = rds_client.describe_db_cluster_endpoints(
        DBClusterIdentifier=f"aurora-cluster-{input_env}-{sandbox_id}",
        Filters=[
            {"Name": "db-cluster-endpoint-type", "Values": [endpoint_type]},
        ],
    )

    return response["DBClusterEndpoints"][0]["Endpoint"]
