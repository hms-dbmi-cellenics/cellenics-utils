import click

from ..utils.AuroraClient import AuroraClient
from ..utils.constants import DEFAULT_AWS_PROFILE, STAGING


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
    help="Local port for the tunnel, default is 5431 for inframock db, 5432 otherwise.",
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
        cellenics rds run psql\n
        cellenics rds run pg_dump > dump.sql
    """

    try:
        with AuroraClient(
            sandbox_id, user, region, input_env, aws_profile, local_port
        ) as client:
            client.run_query(command, capture_output=False)

    except Exception as e:
        print(
            "\n"
            "There was an error connecting to the db:\n"
            f"{e}\n"
            "Try these steps:\n"
            '- Make sure the tunnel is running. If not run "biomage rds tunnel"\n'
            "- If the tunnel is running, try restarting the tunnel\n"
            '- You may need to install psql, run "brew install postgresql"\n'
        )
