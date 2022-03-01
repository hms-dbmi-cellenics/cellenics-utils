# import itertools
from pprint import pprint

# import boto3
import click
from botocore.exceptions import ClientError
from deepdiff import DeepDiff
from PyInquirer import prompt

from ..utils.constants import DEVELOPMENT, PRODUCTION, STAGING

from .start_port_forwarding import start_port_forwarding
from .login import login

@click.group()
def rds():
    """
    Manage Cellenics RDS databases.
    """
    pass

rds.add_command(start_port_forwarding)
rds.add_command(login)