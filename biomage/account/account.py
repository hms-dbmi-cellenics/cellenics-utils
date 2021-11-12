import csv
import os
import string
import sys
import time
from secrets import choice
from subprocess import PIPE, Popen

import click


@click.group()
def account():
    """
    Manage  Cellscope account information.
    """
    pass


userpool = os.getenv("COGNITO_PRODUCTION_POOL")


def generate_password():
    today = time.strftime("%Y-%m-%d")
    return (
        "Tmp_"
        + today
        + "".join([choice(string.ascii_uppercase + string.digits) for _ in range(8)])
    )


def create_account(username, email):
    """
    Creates a new account with the information provided. Requires a password change call afterwards."""
    p = Popen(
        f"""aws cognito-idp admin-create-user \
            --user-pool-id "{userpool}" \
            --username "{email}" \
            --message-action "SUPPRESS" \
            --user-attributes \
                "Name=email,Value={email}" \
                "Name=name,Value='{username}'" \
                "Name=email_verified,Value=true" """.format(
            userpool=userpool, email=email, username=username
        ),
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        shell=True,
    )

    output, error = p.communicate(input=b"\n")

    print(output)

    return error


@click.command()
@click.option(
    "--email",
    required=True,
    help="User email for the account to change the password in production.",
)
@click.option(
    "--password",
    required=True,
    help="Password for the new account.",
)
def change_password(email, password):

    return _change_password(email, password)


def _change_password(email, password):
    p = Popen(
        f"""aws cognito-idp admin-set-user-password \
            --user-pool-id "{userpool}" \
            --username "{email}" \
            --password "{password}" \
            --permanent""".format(
            userpool=userpool, email=email, password=password
        ),
        stdin=PIPE,
        stdout=PIPE,
        stderr=PIPE,
        shell=True,
    )
    output, error = p.communicate(input=b"\n")
    print(output)

    return error


@click.command()
@click.option(
    "--email",
    required=True,
    help="User email for the account.",
)
@click.option(
    "--username",
    required=True,
    help="The first and last name for the user. (e.g.: Arthur Dent)",
)
@click.option(
    "--password",
    required=False,
    help="Password for the new account.",
)
def create_user(username, email, password):
    """
    Creates a new account with the provided password. The user will not receive any
    email and the account & email will be marked as verified.
    """
    if not password:
        password = generate_password()

    print("%s,%s,%s" % (username, email, password))
    _create_user(username, email, password)


def _create_user(username, email, password):

    # format username into title and email into lowercase
    username = username.title()
    email = email.lower()

    error = create_account(username, email)
    # if the user already exists, just proceed and change the password
    # this way, when there's an error creating a list you can just
    # re-run the whole script and get the correct tmp passwords
    if error and "UsernameExistsException" not in str(error):
        return error

    error = _change_password(email, password)
    if error:
        return error


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
def create_users_list(user_list, header):
    """
    Creates a new account for each row in the user_list file.
    The file should be in csv format.
    The first column should be the username in the format: first_name last_name
    The second row should be the email.
    E.g.: Arthur Dent,arthur_dent@galaxy.gl
    """
    import pandas as pd

    with open(user_list + ".out", "w") as out:
        df = pd.read_csv(user_list, header=header, quoting=csv.QUOTE_ALL)
        for _, username, email in df.itertuples():
            password = generate_password()

            username = username.title()
            email = email.lower()
            print("%s,%s,%s" % (username, email, password))
            out.write("%s,%s,%s\n" % (username, email, password))

            error = _create_user(username, email, password)
            if error:
                print(error)
                sys.exit(1)


account.add_command(create_user)
account.add_command(change_password)
account.add_command(create_users_list)
