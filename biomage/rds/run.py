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
@click.option(
    "-lp",
    "--local_port",
    required=False,
    default=None,
    show_default=True,
    help="Port to use locally for the tunnel, default is 5431 for inframock db, 5432 otherwise.",
)
@click.option(
    "-p",
    "--aws_profile",
    required=False,
    default=DEFAULT_AWS_PROFILE,
    show_default=True,
    help="The name of the profile stored in ~/.aws/credentials to use.",
)
@click.argument("command")
def run(command, sandbox_id, input_env, user, region, local_port, aws_profile):
    """
    Runs the provided command in the cluster using IAM if necessary.
    Use 'psql' to start an interactive session.
    Use 'pg_dump' to get a dump of the database.\n

    Examples.:\n
        biomage rds run psql\n
        biomage rds run pg_dump > dump.sql
    """

    try:
        run_rds_command(
            command, sandbox_id, input_env, user, region, aws_profile, local_port
        )
    except Exception:
        print(
            "\n"
            "There was an error connecting to the db. Try these steps:\n"
            '- Make sure the tunnel is running. If not run "biomage rds tunnel"\n'
            "- If the tunnel is running, try restarting the tunnel\n"
            '- You may need to install psql, run "brew install postgresql"\n'
        )


def run_rds_command(
    command,
    sandbox_id,
    input_env,
    user,
    region,
    aws_profile,
    local_port=None,
    capture_output=False,
):
    aws_session = boto3.Session(profile_name=aws_profile)

    password = None

    if input_env == "development":
        password = "password"
        local_port = local_port or 5431
    else:
        local_port = local_port or 5432

        rds_client = aws_session.client("rds")

        remote_endpoint = get_rds_endpoint(
            input_env, sandbox_id, rds_client, ENDPOINT_TYPE
        )

        print(
            f"Generating temporary token for {input_env}-{sandbox_id}", file=sys.stderr
        )
        password = rds_client.generate_db_auth_token(
            remote_endpoint, 5432, user, region
        )

    print("Token generated", file=sys.stderr)

    result = None

    if capture_output:
        result = sub_run(
            f'PGPASSWORD="{password}" {command} \
                --host=localhost \
                --port={local_port} \
                --username={user} \
                --dbname=aurora_db',
            capture_output=True,
            text=True,
            shell=True,
        )
    else:
        result = sub_run(
            f'PGPASSWORD="{password}" {command} \
                --host=localhost \
                --port={local_port} \
                --username={user} \
                --dbname=aurora_db',
            shell=True,
        )

    if result.returncode != 0:
        raise Exception(result.stderr)

    if capture_output:
        return result.stdout


def get_rds_endpoint(input_env, sandbox_id, rds_client, endpoint_type):
    response = rds_client.describe_db_cluster_endpoints(
        DBClusterIdentifier=f"aurora-cluster-{input_env}-{sandbox_id}",
        Filters=[
            {"Name": "db-cluster-endpoint-type", "Values": [endpoint_type]},
        ],
    )

    return response["DBClusterEndpoints"][0]["Endpoint"]
