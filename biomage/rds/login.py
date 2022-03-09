from subprocess import DEVNULL, run

import boto3
import click

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
def login(input_env, user, region):
    """
    Logs into a database using psql and IAM if necessary.\n

    E.g.:
    biomage rds login
    """
    password = None

    internal_port = 5432

    if input_env == "development":
        password = "password"
        internal_port = 5431
    else:
        rds_client = boto3.client("rds")

        remote_endpoint = get_rds_endpoint(input_env, rds_client, endpoint_type)

        print(f"Generating temporary token for {input_env}")
        password = rds_client.generate_db_auth_token(
            remote_endpoint, internal_port, user, region
        )

    print("Token generated")

    result = run(
        f'PGPASSWORD="{password}" psql \
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


def get_rds_endpoint(input_env, rds_client, endpoint_type):
    response = rds_client.describe_db_cluster_endpoints(
        DBClusterIdentifier=f"aurora-cluster-{input_env}",
        Filters=[
            {"Name": "db-cluster-endpoint-type", "Values": [endpoint_type]},
        ],
    )

    return response["DBClusterEndpoints"][0]["Endpoint"]
