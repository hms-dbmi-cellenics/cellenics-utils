import base64
import hashlib
import json
import os
import re
from collections import namedtuple
from functools import reduce

import anybase32
import boto3
import click
import requests
import yaml
from github import Github
from inquirer import Checkbox, Confirm, Text, prompt
from inquirer.themes import GreenPassion

from ..utils.staging import check_if_sandbox_exists

SANDBOX_NAME_REGEX = re.compile(r"^[a-z0-9][-a-z0-9]*[a-z0-9]$")

DEFAULT_BRANCH = "master"


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def get_manifests(templates, pins, token, repo_to_ref):
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
            if recursive_get(document, "spec", "chart", "ref"):
                if name in pins:
                    document["spec"]["chart"]["ref"] = get_branch_ref(
                        document["spec"]["chart"],
                        token,
                        repo_to_ref=repo_to_ref,
                        return_sha=True,
                    )
                else:
                    document["spec"]["chart"]["ref"] = get_branch_ref(
                        document["spec"]["chart"],
                        token,
                        repo_to_ref=repo_to_ref,
                        return_sha=False,
                    )

            manifests.append(document)

    manifests = yaml.dump_all(manifests)

    return manifests


def download_templates(org, repo, ref):
    # If no pull request ID was specified in the command.
    if isinstance(ref, int):
        template = f"refs-pull-{ref}-merge.yaml"
    elif isinstance(ref, str):
        template = f"refs-heads-{ref}.yaml"
    elif not ref:
        template = f"refs-heads-{DEFAULT_BRANCH}.yaml"
    else:
        raise Exception("Ref must be integer, string, or None.")

    url = (
        f"https://raw.githubusercontent.com/{org}/releases/master/"
        f"staging-candidates/{repo}/{template}"
    )

    s = requests.Session()
    r = s.get(url)
    Deployment = namedtuple("Deployment", ["ref", "url", "status", "text"])

    return Deployment(
        ref=ref or DEFAULT_BRANCH, url=url, status=r.status_code, text=r.text
    )


def compile_requirements(org, deployments):
    REPOS = ("ui", "api", "worker", "pipeline")

    repo_to_ref = {deployment: None for deployment in REPOS}

    for deployment in deployments:
        try:
            repo, ref = deployment.split("/", 1)
        except ValueError:
            repo_to_ref[deployment] = None
            continue

        try:
            repo_to_ref[repo] = int(ref)
        except ValueError:
            repo_to_ref[repo] = ref

    templates = {}
    for repo, ref in repo_to_ref.items():
        templates[repo] = download_templates(org, repo, ref)

    click.echo(
        click.style(
            "Deployments fetched:",
            bold=True,
        )
    )
    click.echo(
        "{0:<15}{1:<10}{2:<10}{3}".format("Repository", "Ref", "Status", "Manifest URL")
    )

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

    return templates, repo_to_ref


def get_branch_ref(chart, token, repo_to_ref=None, return_sha=False):
    """
    Get a reference to a branch given the chart information (git, path, ref)
    supplied.


    `repo_to_ref` is a dictionary where each key is a repo and the value
     can be an integer (referring to a pull request, e.g. `api/22`), a
    string (referring to a branch, e.g. `api/master`) or Null, which indicates
    the default branch for the repository specified.

    If return_sha is True, this returns the SHA at the head of the default
    branch. If it is False, it returns the name of the default branch.
    """

    # A `git` reference can be https://github.com/biomage-org/releases
    # Here we extract the repository and organization from the string.
    path = chart["git"].split(":")
    org, repo_name = path[1].split("/")[-2:]

    g = Github(token)
    org = g.get_organization(org)
    repo = org.get_repo(repo_name)

    # We set the reference here according to the chart repo, not the repo
    # to be released to avoid pointing to invalid references for repos whose
    # charts are in IAC.
    ref = None
    if repo_name in repo_to_ref:
        ref = repo_to_ref[repo_name]

    if isinstance(ref, int):
        target_branch = f"refs/pull/{ref}/head"
    elif isinstance(ref, str):
        target_branch = f"refs/heads/{ref}"
    else:
        target_branch = f"refs/heads/{repo.default_branch}"

    # if no specific reference was specified (e.g. `api` instead of `api/22`)
    # and no SHA was requested, return the name of the branch
    if not return_sha:
        return target_branch

    for ref in repo.get_git_refs():
        if ref.ref == target_branch:
            return ref.object.sha

    raise Exception("Invalid repository supplied.")


def get_sandbox_id(templates, manifests, org, auto=False):
    # Generate a sandbox name and ask the user what they want theirs to be called.
    manifest_hash = hashlib.md5(manifests.encode()).digest()
    manifest_hash = anybase32.encode(manifest_hash, anybase32.ZBASE32).decode()
    pr_ids = "-".join(
        [
            f"{repo}{opts.ref}"
            for repo, opts in templates.items()
            if opts.ref != DEFAULT_BRANCH
        ]
    )
    user_name = re.sub(r"[^\w\s]", "", os.getenv("BIOMAGE_NICK", os.getenv("USER", "")))

    fragments = (
        user_name,
        pr_ids if pr_ids else manifest_hash,
    )
    sandbox_id = "-".join([bit for bit in fragments if bit]).lower()[:26]

    if auto:
        return sandbox_id

    # Ask the user to provide one if they want
    click.echo()
    click.echo(click.style("Give a sandbox ID.", fg="yellow", bold=True))
    click.echo(
        "The sandbox ID must :\n"
        "- be no more than 26 characters long\n"
        "- consist only of lower case alphanumeric characters and -.\n"
        "- start and end with an alphanumeric character\n"
        "- be different from an existing sandbox IDs\n"
        "\n"
        "A unique ID generated from the contents of the deployments and your pinning\n"
        "choices has been provided as a default."
    )
    while True:
        questions = [
            Text(
                name="sandbox_id",
                message="Provide an ID:",
                default=sandbox_id,
            )
        ]

        click.echo()
        answer = prompt(questions, theme=GreenPassion())
        sandbox_id = answer["sandbox_id"]

        if len(sandbox_id) > 26:
            click.echo(click.style("Sandbox ID is more than 26 characters.", fg="red"))
            continue

        if not SANDBOX_NAME_REGEX.match(sandbox_id):
            click.echo(click.style("Check the syntax of your sandbox id.", fg="red"))
            continue

        if check_if_sandbox_exists(org, sandbox_id):
            click.echo(click.style("A sandbox with this ID exists.", fg="red"))
            continue

        return sandbox_id


def create_manifest(templates, token, org, repo_to_ref, auto=False, with_rds=False):

    # autopin the repos on the default branch
    if auto:
        pins = [
            name for name, props in templates.items() if props.ref == DEFAULT_BRANCH
        ]
    else:
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
            "The sandbox will not be affected by any future changes made to pinned "
            "deployments. For example, if you pin `ui`, \n"
            f"no new changes made to the {DEFAULT_BRANCH} branch of the `ui` "
            "repository will propagate to your sandbox after it’s created.\n"
            f"By default, only deployments sourced from the {DEFAULT_BRANCH} "
            "are pinned, deployments using branches you are "
            "likely to be testing (e.g. pull requests) are not.",
        )
        questions = [
            Checkbox(
                name="pins",
                message="Which deployments would you like to pin?",
                choices=[
                    name for name in templates.keys()
                ],
                default=[
                    name for name, props in templates.items() if props.ref == DEFAULT_BRANCH
                ]
            )
        ]

        click.echo()
        answer = prompt(questions, theme=GreenPassion())
        pins = set(answer["pins"])

    if len(pins) > 0:
        click.echo("Pinned repositories:")
        click.echo("\n".join(f"• {pin}" for pin in pins))
    else:
        click.echo("Not pinning any repository")

    # Find the latest SHA of the iac
    # Generate a list of manifests from all the url's we collected.
    manifests = get_manifests(templates, pins, token, repo_to_ref)

    # Write sandbox ID
    sandbox_id = get_sandbox_id(templates, manifests, org, auto=auto)

    # Decide the RDS cluster ID
    rds_sandbox_id = sandbox_id if with_rds else "default"

    click.echo()
    manifests = manifests.replace("STAGING_SANDBOX_ID", sandbox_id)
    manifests = manifests.replace("STAGING_RDS_SANDBOX_ID", rds_sandbox_id)
    manifests = base64.b64encode(manifests.encode()).decode()

    return manifests, sandbox_id


@click.command()
@click.argument("deployments", nargs=-1)
@click.option(
    "--token",
    "-t",
    envvar="GITHUB_API_TOKEN",
    required=True,
    show_default=True,
    help="A GitHub Personal Access Token with the required permissions.",
)
@click.option(
    "--with_rds",
    is_flag=True,
    default=False,
    show_default=True,
    help="Stage a new RDS instance for this staging environment.",
)
@click.option(
    "--org",
    envvar="GITHUB_BIOMAGE_ORG",
    default="biomage-org",
    show_default=True,
    help="The GitHub organization to perform the operation in.",
)
@click.option(
    "--auto",
    is_flag=True,
    default=False,
    show_default=True,
    help="Set auto flag to use default staging options without requiring any"
    " user input. It will pin the default branches, generate a random name for"
    " the sandbox, and will not stage any experiment data.",
)
def stage(token, org, deployments, with_rds, auto):
    """
    Deploys a custom staging environment.
    """

    # generate templats
    templates, repo_to_ref = compile_requirements(org, deployments)

    manifest, sandbox_id = create_manifest(
        templates,
        token,
        org,
        repo_to_ref,
        auto=auto,
        with_rds=with_rds,
    )

    click.echo()
    click.echo(f"Sandbox ID: {sandbox_id}")

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

    if not auto:
        questions = [
            Confirm(
                name = "create",
                message = "Are you sure you want to create this deployment?",
                default = False,
            )
        ]
        click.echo()
        answer = prompt(questions, theme=GreenPassion())
        if not answer["create"]:
            exit(1)

    g = Github(token)
    o = g.get_organization(org)
    r = o.get_repo("iac")

    wf = None
    for workflow in r.get_workflows():
        if workflow.name == "Deploy a staging environment":
            wf = str(workflow.id)

    wf = r.get_workflow(wf)

    workflow_started = wf.create_dispatch(
        ref="master",
        inputs={
            "manifest": manifest,
            "sandbox-id": sandbox_id,
            # Convert stage_rds to string because Github has issues with boolean inputs
            # https://github.com/actions/runner/issues/1483
            "with-rds": str(with_rds),
            "secrets": secrets,
        },
    )

    if not workflow_started:
        click.echo(
            click.style(
                "❌ Could not run workflow. Does your GitHub token have the required "
                f"privileges? See https://github.com/{org}/biomage-utils#setup for"
                " more information.",
                fg="red",
                bold=True,
            )
        )
        return

    click.echo()
    click.echo(
        click.style(
            "✔️ Deployment submitted. You can check your progress at "
            f"https://github.com/{org}/iac/actions. When the deployment is done"
            " run the following command to trigger flux synchronization and "
            " speed up the process:",
            fg="green",
            bold=True,
        )
    )

    click.echo()
    click.echo(
        "\tfluxctl sync --k8s-fwd-ns flux --context arn:aws:eks:eu-west-1:"
        "242905224710:cluster/biomage-staging",
    )
    click.echo()

    click.echo(
        click.style(
            "✔️ The deployment, when done, should be available at "
            f"https://ui-{sandbox_id}.scp-staging.biomage.net/",
            fg="green",
            bold=True,
        )
    )
