# import itertools
from pprint import pprint

# import boto3
import click
from botocore.exceptions import ClientError
from deepdiff import DeepDiff
from PyInquirer import prompt

from ..utils.constants import DEVELOPMENT, PRODUCTION, STAGING
from .login import login
from .tunnel import tunnel


@click.group()
def rds():
    """
    Manage Cellenics RDS databases.
    """
    pass


rds.add_command(tunnel)
rds.add_command(login)
