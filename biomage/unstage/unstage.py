import click
import requests
import boto3
import json
import base64
from PyInquirer import prompt
from github import Github


def check_if_exists(org, sandboxid):
    url = f"https://raw.githubusercontent.com/{org}/iac/master/releases/staging/{sandboxid}.yaml"  # noqa: E501

    s = requests.Session()
    r = s.get(url)

    return 200 <= r.status_code < 300


@click.command()
@click.argument("sandboxid", nargs=1)
@click.option(
    "--token",
    "-t",
    envvar="GITHUB_API_TOKEN",
    required=True,
    help="A GitHub Personal Access Token with the required permissions.",
)
@click.option(
    "--org",
    envvar="GITHUB_BIOMAGE_ORG",
    default="biomage-ltd",
    help="The GitHub organization to perform the operation in.",
)
def unstage(token, org, sandboxid):
    """
    Removes a custom staging environment.
    """

    if not check_if_exists(org, sandboxid):
        click.echo()
        click.echo(
            click.style(
                f"✖️ Staging sandbox with ID `{sandboxid}` could not be found.",
                fg="red",
                bold=True,
            )
        )
        exit(1)

    # get (secret) access keys
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()

    credentials = {
        "access_key": credentials.access_key,
        "secret_key": credentials.secret_key,
        "github_api_token": token,
    }

    # encrypt (secret) access keys
    kms = boto3.client("kms")
    secrets = kms.encrypt(
        KeyId="alias/iac-secret-key", Plaintext=json.dumps(credentials).encode()
    )
    secrets = base64.b64encode(secrets["CiphertextBlob"]).decode()

    questions = [
        {
            "type": "confirm",
            "name": "delete",
            "default": False,
            "message": "Are you sure you want to remove the sandbox "
            f"with ID `{sandboxid}`. This cannot be undone.",
        }
    ]
    click.echo()
    answers = prompt(questions)
    if not answers["delete"]:
        exit(1)

    g = Github(token)
    o = g.get_organization(org)
    r = o.get_repo("iac")

    wf = None
    for workflow in r.get_workflows():
        if workflow.name == "Remove a staging environment":
            wf = str(workflow.id)

    wf = r.get_workflow(wf)

    wf.create_dispatch(
        ref="master",
        inputs={"sandbox-id": sandboxid, "secrets": secrets},
    )

    click.echo()
    click.echo(
        click.style(
            "✔️ Removal submitted. You can check your progress at "
            f"https://github.com/{org}/iac/actions",
            fg="green",
            bold=True,
        )
    )
