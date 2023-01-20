import csv
import os
import re
import string
import sys
import time
from secrets import choice

import biomage_programmatic_interface as bpi
import boto3
import click
import pandas as pd

from ..utils.constants import DEFAULT_AWS_PROFILE, PRODUCTION, STAGING


@click.group()
def account():
    """
    Manage Cellenics account information.
    """
    pass


COGNITO_PRODUCTION_POOL = os.getenv("COGNITO_PRODUCTION_POOL")
COGNITO_STAGING_POOL = os.getenv("COGNITO_STAGING_POOL")


def generate_password():
    today = time.strftime("%Y-%m-%d")
    return (
        "Tmp_"
        + today
        + "".join([choice(string.ascii_uppercase + string.digits) for _ in range(8)])
    )


def create_account(full_name, email, aws_profile, userpool):
    """
    Creates a new account with the information provided.
    Requires a password change call afterwards."""

    session = boto3.Session(profile_name=aws_profile)
    cognito = session.client("cognito-idp")

    cognito.admin_create_user(
        UserPoolId=userpool,
        Username=email,
        MessageAction="SUPPRESS",
        UserAttributes=[
            {"Name": "email", "Value": email},
            {"Name": "name", "Value": full_name},
            {"Name": "email_verified", "Value": "true"},
        ],
    )


@click.command()
@click.option(
    "-e",
    "--email",
    required=True,
    help="User email for the account to change the password in production.",
)
@click.option(
    "-P",
    "--password",
    required=True,
    help="New password for the account.",
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
    "-u",
    "--userpool",
    required=True,
    help="Userpool of the account to change.",
)
def change_password(email, password, aws_profile, userpool):
    print(
        "Changing password for %s to %s in user pool %s..."
        % (email, password, userpool)
    )

    try:
        _change_password(email, password, aws_profile, userpool)
    except Exception as error:
        print("Error changing password: %s" % error)


def _change_password(email, password, aws_profile, userpool):
    session = boto3.Session(profile_name=aws_profile)
    cognito = session.client("cognito-idp")

    cognito.admin_set_user_password(
        UserPoolId=userpool, Username=email, Password=password, Permanent=True
    )


@click.command()
@click.option(
    "-e",
    "--email",
    required=True,
    help="User email for the account.",
)
@click.option(
    "-n",
    "--full_name",
    required=True,
    help="The first and last name for the user. (e.g.: Arthur Dent)",
)
@click.option(
    "-P",
    "--password",
    required=False,
    help="Password for the new account.",
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
    "-u",
    "--userpool",
    required=True,
    help="Userpool to add the new account to.",
)
def create_user(full_name, email, password, aws_profile, userpool):
    """
    Creates a new account with the provided password. The user will not receive any
    email and the account & email will be marked as verified.
    """
    if not password:
        password = generate_password()

    error = _validate_input(email, full_name)
    if error:
        print(error)
        sys.exit(1)

    error = _create_user(full_name, email, password, userpool, aws_profile)
    if error:
        print("Error creating user: %s" % error)


def _create_user(full_name, email, password, userpool, aws_profile, overwrite=False):

    # format full_name into title and email into lowercase
    full_name = full_name.title()
    email = email.lower()

    try:
        create_account(full_name, email, aws_profile, userpool)
    except Exception as error:
        if error and not ("UsernameExistsException" in str(error) and overwrite):
            return error

    try:
        _change_password(email, password, aws_profile, userpool)
    except Exception as error:
        return error


def _validate_input(email, full_name):
    """
    Check if the information provided for user creation is valid:
    - Email is valid (according to regex in https://emailregex.com/)
    - Full name is provided
    Returns error message if any of the checks fail
    """
    if not email or pd.isna(email):
        return f"ERROR: Email not provided for user {full_name}"

    if not full_name or pd.isna(full_name):
        return f"ERROR: Full name not provided for user {email}"

    if not re.match(r"(^[a-zA-Z0-9_.\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\.\-]+$)", email):
        return f"ERROR: Email {email} does not match regex"


@click.command()
@click.option(
    "--user_list",
    required=True,
    help="User list containing user and email for the new accounts in csv.",
)
@click.option(
    "--header",
    required=False,
    default=None,
    help="""Header parameter passed to pandas read_csv function. Use 'None'
    if no headers are present, otherwise specify the header row number with an int.""",
)
@click.option(
    "-i",
    "--input_env",
    required=False,
    default=STAGING,
    show_default=True,
    help="Input environment to pull the data from.",
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
    "--overwrite",
    required=False,
    default=False,
    show_default=True,
    help="Overwrite password for existing user accounts.",
)
def create_users_list(user_list, header, input_env, aws_profile, overwrite):
    """
    Creates a new account for each row in the user_list file.
    The file should be in csv format.
    The first column should be the full_name in the format: first_name last_name
    The second column should be the email.
    E.g.: Arthur Dent,arthur_dent@galaxy.gl
    """
    _create_users_list_func(user_list, header, input_env, aws_profile, overwrite)


def _create_users_list_func(user_list, header, input_env, aws_profile, overwrite):

    userpool = None
    if input_env == PRODUCTION:
        userpool = COGNITO_PRODUCTION_POOL
    elif input_env == STAGING:
        userpool = COGNITO_STAGING_POOL

    with open(user_list + ".out", "w") as out:
        df = pd.read_csv(user_list, header=header, quoting=csv.QUOTE_ALL)
        for _, full_name, email in df.itertuples():

            full_name = full_name.title().strip()
            email = email.lower().strip()

            error = _validate_input(email, full_name)
            if error:
                print(error)
                sys.exit()

            password = generate_password()

            error = _create_user(
                full_name, email, password, userpool, aws_profile, overwrite
            )

            if error and not ("UsernameExistsException" in str(error) and overwrite):
                out.write("%s,%s,Already have an account\n" % (full_name, email))
                continue

            if error:
                print("Error creating user {email} with password {password}")
                print(error)
                sys.exit(1)

            print("%s,%s,%s" % (full_name, email, password))
            out.write("%s,%s,%s\n" % (full_name, email, password))


def _create_process_experiment(experiment_name, user_email, user_password, samples_path, instance_url):
    connection = bpi.Connection(user_email, user_password, instance_url)
    experiment = connection.create_experiment(experiment_name)
    experiment.upload_samples(samples_path)


@click.command()
@click.option(
    "--user_list",
    required=True,
    help="User list containing user and email for the new accounts in csv.",
)
@click.option(
    "--experiment_name",
    required=True,
    help="Experiment name to be created",
)
@click.option(
    "--samples_path",
    required=True,
    help="Local path to the samples for upload",
)
@click.option(
    "--instance_url",
    required=False,
    default='https://api-default.scp-staging.biomage.net/',
    help="URL of the cellenics api",
)
@click.option(
    "-p",
    "--aws_profile",
    required=False,
    default=DEFAULT_AWS_PROFILE,
    show_default=True,
    help="The name of the profile stored in ~/.aws/credentials to use.",
)
def create_process_experiment_list(experiment_name, user_list, samples_path, instance_url, aws_profile):
    """
    Creates users, using the user_list file.
    Creates experiment, uploads samples and processes the projet for each row in the user_list file.
    The file should be in csv format.
    The first column should be the full_name in the format: first_name last_name
    The second column should be the email.
    E.g.: Arthur Dent, arthur_dent@galaxy.gl
    """
    cognito_pool = COGNITO_STAGING_POOL

    # creating the users
    _create_users_list_func(user_list, None, 'staging', aws_profile, False)
    session = boto3.Session(profile_name=aws_profile)
    client = session.client('cognito-idp')
    created_users = pd.read_csv(user_list + ".out", header=None, quoting=csv.QUOTE_ALL)

    for _, name, email, password in created_users.itertuples():
        # Update the user's "custom:agreed_terms" attribute
        response2 = client.admin_update_user_attributes(
            UserPoolId=cognito_pool,
            Username=email,
            UserAttributes=[
                {
                    'Name': 'custom:agreed_terms',
                    'Value': 'true',
                },
            ]
        )
        print(response2)
        _create_process_experiment(experiment_name, email, password, samples_path, instance_url)

        # update agreed_terms to 'false' again
        response2 = client.admin_update_user_attributes(
            UserPoolId=cognito_pool,
            Username=email,
            UserAttributes=[
                {
                    'Name': 'custom:agreed_terms',
                    'Value': 'false',
                },
            ]
        )


account.add_command(create_user)
account.add_command(change_password)
account.add_command(create_users_list)
account.add_command(create_process_experiment_list)