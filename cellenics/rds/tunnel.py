import pathlib
import signal
from subprocess import DEVNULL, run

import click

from ..utils.constants import DEFAULT_AWS_PROFILE, STAGING


def force_exit_handler(signum, frame):
    file_dir = pathlib.Path(__file__).parent.resolve()
    run(f"{file_dir}/cleanup_tunnel.sh", shell=True)
    exit()


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
    "-lp",
    "--local_port",
    required=False,
    default=5432,
    show_default=True,
    help="Port to use locally for the tunnel.",
)
@click.option(
    "-p",
    "--aws_profile",
    required=False,
    default=DEFAULT_AWS_PROFILE,
    show_default=True,
    help="The name of the profile stored in ~/.aws/credentials to use.",
)
@click.option(
    "-v",
    "--verbose",
    required=False,
    default=True,
    show_default=True,
    help="Verbosity of the command.",
)
def tunnel(input_env, region, sandbox_id, local_port, aws_profile, verbose):
    """
    Sets up an ssh tunneling/port forwarding session
    for the rds server in a given environment.\n

    E.g.:
    cellenics rds tunnel -i staging
    """

    open_tunnel(input_env, region, sandbox_id, local_port, aws_profile, verbose=verbose)

    input(
        """
Finished setting up, run \"cellenics rds run psql -i $ENVIRONMENT -s $SANDBOX_ID -r
 $REGION -p $AWS_PROFILE\" in a different tab

------------------------------
Press enter to close session.
------------------------------
"""
    )

    close_tunnel()


def open_tunnel(input_env, region, sandbox_id, local_port, aws_profile, verbose=False):
    signal.signal(signal.SIGINT, force_exit_handler)

    # we use the writer endpoint because the reader endpoint might still connect to
    # the writer endpoint when there's a single instance and provide a false
    # sense of safety
    endpoint_type = "writer"
    file_dir = pathlib.Path(__file__).parent.resolve()

    run(
        [
            f"{file_dir}/tunnel.sh",
            input_env,
            sandbox_id,
            region,
            str(local_port),
            endpoint_type,
            aws_profile,
        ],
        stdout=None if verbose else DEVNULL,
    )


def close_tunnel():
    file_dir = pathlib.Path(__file__).parent.resolve()
    run(f"{file_dir}/cleanup_tunnel.sh", shell=True)
