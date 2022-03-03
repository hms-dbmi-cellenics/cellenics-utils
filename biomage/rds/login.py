from subprocess import run

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
    "-p",
    "--port",
    required=False,
    default=5432,
    show_default=True,
    help="Port of the db.",
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
    help="Role to connect as (role is the same as user).",
)
# Disabled, it doesn't change anything when there is only one instance
# and might lead to confusion
# @click.option(
#     "-t",
#     "--endpoint_type",
#     required=False,
#     default="reader",
#     show_default=True,
#     help="The type of the rds endpoint you want to connect to, can \
#         be either reader or writer",
# )
def login(input_env, port, user, region, endpoint_type="writer"):
    """
    Logs into a database using psql and IAM if necessary.\n

    E.g.:
    biomage rds login
    """
    password = None

    internal_port = port

    if input_env == "development":
        password = "password"
    else:
        internal_port = 5432
        print(
            "Only local port 5432 works connecting to staging and prod for now, \
                so setting it to 5432"
        )

        rds_client = boto3.client("rds")

        remote_endpoint = get_rds_endpoint(input_env, rds_client, endpoint_type)

        print(f"Generating temporary token for {input_env}")
        password = rds_client.generate_db_auth_token(
            remote_endpoint, internal_port, user, region
        )

    print("Token generated")

    run(
        f'PGPASSWORD="{password}" psql \
            --host=localhost \
            --port={internal_port} \
            --username={user} \
            --dbname=aurora_db',
        shell=True,
    )


def get_rds_endpoint(input_env, rds_client, endpoint_type):
    response = rds_client.describe_db_cluster_endpoints(
        DBClusterIdentifier=f"aurora-cluster-{input_env}",
        Filters=[
            {"Name": "db-cluster-endpoint-type", "Values": [endpoint_type]},
        ],
    )

    return response["DBClusterEndpoints"][0]["Endpoint"]
