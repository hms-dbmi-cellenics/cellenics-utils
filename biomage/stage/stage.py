import click
import requests
import json
import yaml
import hashlib
import anybase32
import base64
import boto3
import re
from collections import namedtuple
from PyInquirer import prompt
from github import Github
from functools import reduce

SANDBOX_NAME_REGEX = re.compile(
    r"[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*"
)


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def download_deployments(org, repo, ref):
    if not ref:
        url = f"https://raw.githubusercontent.com/{org}/iac/master/releases/staging-candidates/{repo}/refs-heads-master.yaml"  # noqa: E501
    else:
        url = f"https://raw.githubusercontent.com/{org}/iac/master/releases/staging-candidates/{repo}/refs-pull-{ref}-merge.yaml"  # noqa: E501

    s = requests.Session()
    r = s.get(url)

    Deployment = namedtuple("Deployment", ["ref", "url", "status", "text"])

    return Deployment(ref=ref or "master", url=url, status=r.status_code, text=r.text)


def compile_requirements(org, refs):
    DEPLOYMENTS = ("ui", "api", "worker")

    ref_data = {deployment: None for deployment in DEPLOYMENTS}
    for ref in refs:
        try:
            repo, pr_id = ref.split("/", 1)
            ref_data[repo] = int(pr_id)
        except Exception:
            ref_data[ref] = None

    templates = {}
    for repo, ref in ref_data.items():
        templates[repo] = download_deployments(org, repo, ref)

    click.echo("{0:<15}{1:<10}{2:<10}{3}".format("DEPLOYMENT", "REF", "STATUS", "URL"))

    can_deploy = True
    for repo, (ref, url, status, text) in templates.items():
        success = 200 <= status <= 299

        click.echo(
            click.style(
                f"{repo:<15}{ref:<10}{status:<10}{url}",
                fg="green" if success else "red",
            )
        )

        can_deploy = can_deploy and success

    if not can_deploy:
        click.echo()
        click.echo(
            click.style(
                "✖️ Not all deployment files could be found. "
                "Check the URLs and status codes printed above and try again.",
                fg="red",
                bold=True,
            )
        )
        exit(1)

    return templates


def get_latest_master_sha(chart, token):
    path = chart["git"].split(":")
    org, repo = path[1].split("/")

    g = Github(token)
    org = g.get_organization(org)
    repo = org.get_repo(repo)

    for ref in repo.get_git_refs():
        if ref.ref == "refs/heads/master":
            return ref.object.sha

    raise Exception("Invalid repository supplied.")


def create_manifest(templates, token):
    # Ask about which releases to pin.
    click.echo()
    click.echo(
        click.style(
            "A sandbox will be created from the manifest files listed above. "
            "Now specify which deployments you would like to pin.",
            fg="yellow",
            bold=True,
        )
    )
    click.echo(
        "Pinned deployments are immutable, i.e. the sandbox will not be updated "
        "with new images and charts that may be available for\n"
        "the deployment under the given ref. For example, if you want to ensure "
        "that other developers pushing ui features\n"
        "to the master branch do not update and potentially break your sandbox, "
        "you should pin your ui deployment. By default, \n"
        "only deployments sourced from the master branch are pinned, other refs "
        "you likely want to test (e.g. pull requests) are not."
    )
    questions = [
        {
            "type": "checkbox",
            "name": "pins",
            "message": "Which deployments would you like to pin?",
            "choices": [
                {"name": name, "checked": props.ref == "master"}
                for name, props in templates.items()
            ],
        }
    ]

    click.echo()
    pins = prompt(questions)
    try:
        pins = set(pins["pins"])
    except Exception:
        exit(1)

    # Find the latest SHA of the iac
    # Generate a list of manifests from all the url's we collected.
    manifests = []

    # Open each template and iterate through the documents. If we
    # find a `fluxcd.io/automated` annotation, set it to the appropriate
    # value depending on the pinning request.
    for name, template in templates.items():
        documents = yaml.load_all(template.text, Loader=yaml.SafeLoader)

        for document in documents:
            # disable automatic image fetching if pinning is on
            if recursive_get(
                document, "metadata", "annotations", "fluxcd.io/automated"
            ):
                document["metadata"]["annotations"]["fluxcd.io/automated"] = str(
                    name not in pins
                ).lower()

            # pin chart version if pinning is on
            if (
                recursive_get(document, "spec", "chart", "ref")
                and document["spec"]["chart"]["ref"] == "STAGING_CHART_REF"
            ):
                if name in pins:
                    document["spec"]["chart"]["ref"] = get_latest_master_sha(
                        document["spec"]["chart"], token
                    )
                else:
                    document["spec"]["chart"]["ref"] = "master"

            manifests.append(document)

    manifests = yaml.dump_all(manifests)

    # Generate a sandbox name and ask the user what they want theirs to be called.
    sandbox_id = hashlib.md5(manifests.encode()).digest()
    sandbox_id = anybase32.encode(sandbox_id, anybase32.ZBASE32).decode()

    # Ask the user to provide one if they want
    click.echo()
    click.echo(click.style("Give a sandbox ID.", fg="yellow", bold=True))
    click.echo(
        "The sandbox ID must be no more than 26 characters long, consist of "
        "lower case alphanumeric characters, or `-`, and must\n"
        "start and end with an alphanumeric character. A unique ID generated from "
        "the contents of the deployments and your pinning\n"
        "choices has been provided as a default."
    )
    questions = [
        {
            "type": "input",
            "name": "sandbox_id",
            "message": "Provide an ID:",
            "default": sandbox_id,
            "validate": lambda i: SANDBOX_NAME_REGEX.match(i) and len(i) <= 26,
        }
    ]

    click.echo()
    sandbox_id = prompt(questions)
    try:
        sandbox_id = sandbox_id["sandbox_id"]
    except Exception:
        exit(1)

    # Write sandbox ID
    manifests = manifests.replace("STAGING_SANDBOX_ID", sandbox_id)

    return manifests, sandbox_id


@click.command()
@click.argument("refs", nargs=-1)
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
def stage(token, org, refs):
    """
    Deploys a custom staging environment.
    """

    # generate templats
    templates = compile_requirements(org, refs)
    manifest, sandbox_id = create_manifest(templates, token)
    manifest = base64.b64encode(manifest.encode()).decode()

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
            "name": "create",
            "message": "Are you sure you want to create this deployment?",
        }
    ]
    click.echo()
    answers = prompt(questions)
    if not answers["create"]:
        exit(1)

    g = Github(token)
    o = g.get_organization(org)
    r = o.get_repo("iac")

    wf = None
    for workflow in r.get_workflows():
        if workflow.name == "Deploy a staging environment":
            wf = str(workflow.id)

    wf = r.get_workflow(wf)

    wf.create_dispatch(
        ref="master",
        inputs={"manifest": manifest, "sandbox-id": sandbox_id, "secrets": secrets},
    )

    click.echo()
    click.echo(
        click.style(
            "✔️ Deployment submitted. You can check your progress at "
            f"https://github.com/{org}/iac/actions",
            fg="green",
            bold=True,
        )
    )
    click.echo(
        click.style(
            "✔️ The deployment, when done, should be available at "
            f"https://ui-{sandbox_id}.scp-staging.biomage.net/",
            fg="green",
            bold=True,
        )
    )
