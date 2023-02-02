import json
import time
from functools import reduce

import boto3
import cfn_flip
import click
import requests
from botocore.config import Config
from github import Github

from ..utils.encrypt import encrypt
from inquirer import Confirm, prompt
from inquirer.themes import GreenPassion


config = Config(
    region_name="eu-west-1",
)


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def filter_iam_repos(repo):
    if repo.archived:
        return False

    # get files in root
    contents = repo.get_contents("")

    for content in contents:
        # search for tags.y(a)ml file
        if content.path != ".ci.yml" and content.path != ".ci.yaml":
            continue

        # open contents
        tags = cfn_flip.to_json(content.decoded_content)

        tags = json.loads(tags)

        if recursive_get(tags, "ci-policies"):
            return repo.name, recursive_get(tags, "ci-policies")

        return False

    return False


# CF template names can't contain underscores or dashes, remove them and capitalize
# the string
def format_name_for_cf(repo_name):
    return repo_name.replace("_", " ").replace("-", " ").title().replace(" ", "")


def create_new_iam_users(policies):
    users = {}

    for repo, policies in policies.items():
        users[f"{format_name_for_cf(repo)}CIUser"] = {
            "Path": f"/ci-users/{repo}/",
            "UserName": f"ci-user-{repo}",
            "Policies": policies,
        }

    stack_cfg = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Set up GitHub CI users with appropriate rights "
        "[managed by github.com/biomage-utils, command `biomage rotate-ci`]",
        "Resources": {
            name: {"Type": "AWS::IAM::User", "Properties": properties}
            for name, properties in users.items()
        },
    }

    stack_cfg = cfn_flip.to_yaml(json.dumps(stack_cfg))
    cf = boto3.client("cloudformation", config=config)

    kwargs = {
        "StackName": "biomage-ci-users",
        "TemplateBody": stack_cfg,
        "Capabilities": ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
    }

    try:
        stack = cf.create_stack(**kwargs)
    except Exception as e:
        if "AlreadyExistsException" in str(e):
            try:
                stack = cf.update_stack(**kwargs)
            except Exception as e:
                if "No updates are to be performed" in str(e):
                    click.echo("All users are up to date.")
                    return
                else:
                    raise e
        else:
            raise e

    click.echo(
        "Now creating CloudFormation stack. Waiting for completion...",
        nl=False,
    )

    while True:
        time.sleep(10)
        response = cf.describe_stacks(StackName=stack["StackId"])

        status = response["Stacks"][0]["StackStatus"]

        if "FAILED" in status or "ROLLBACK" in status or "DELETE" in status:
            click.echo()
            click.echo(
                click.style(
                    f"✖️ Stack creation failed with error {status}. "
                    "Check the AWS Console for more details.",
                    fg="red",
                    bold=True,
                )
            )
            exit(1)
        elif "COMPLETE" in status:
            click.echo()
            click.echo(f"Stack successfully created with status {status}.")
            break
        else:
            click.echo(".", nl=False)

    click.echo("Created new users.")


def create_new_access_keys(iam, roles):
    click.echo("Now creating new access keys for users...")
    keys = {}

    for repo in roles:
        key = iam.create_access_key(UserName=f"ci-user-{repo}")
        keys[repo] = (
            key["AccessKey"]["AccessKeyId"],
            key["AccessKey"]["SecretAccessKey"],
        )

    return keys


def update_github_secrets(keys, token, org):
    click.echo("Now updating all repositories with new keys...")

    s = requests.Session()
    s.headers = {"Authorization": f"token {token}", "User-Agent": "Requests"}
    url_base = f"https://api.github.com/repos/{org.login}"

    results = {}

    for repo_name, (access_key_id, secret_access_key) in keys.items():
        ci_keys = s.get(f"{url_base}/{repo_name}/actions/secrets/public-key")

        if ci_keys.status_code != requests.codes.ok:
            results[repo_name] = ci_keys.status_code
            continue

        ci_keys = ci_keys.json()

        access_key_id = encrypt(ci_keys["key"], access_key_id)
        secret_access_key = encrypt(ci_keys["key"], secret_access_key)
        encrypted_token = encrypt(ci_keys["key"], token)

        r = s.put(
            f"{url_base}/{repo_name}/actions/secrets/AWS_ACCESS_KEY_ID",
            json={"encrypted_value": access_key_id, "key_id": ci_keys["key_id"]},
        )

        r = s.put(
            f"{url_base}/{repo_name}/actions/secrets/AWS_SECRET_ACCESS_KEY",
            json={"encrypted_value": secret_access_key, "key_id": ci_keys["key_id"]},
        )

        r = s.put(
            f"{url_base}/{repo_name}/actions/secrets/API_TOKEN_GITHUB",
            json={"encrypted_value": encrypted_token, "key_id": ci_keys["key_id"]},
        )

        results[repo_name] = r.status_code

    return results


def rollback_if_necessary(iam, keys, result_codes):
    click.echo("Results for each repository:")

    success = True

    click.echo(
        "{0:<15}{1:<25}{2:<15}".format("REPOSITORY", "UPDATE STATUS (HTTP)", "STATUS")
    )
    for repo, code in result_codes.items():

        status = None
        username = f"ci-user-{repo}"
        generated_key_id, _ = keys[repo]

        if not 200 <= code <= 299:
            iam.delete_access_key(UserName=username, AccessKeyId=generated_key_id)
            status = "Key rolled back"
            success = False
        else:
            user_keys = iam.list_access_keys(UserName=username)
            user_keys = user_keys["AccessKeyMetadata"]

            keys_deleted = 0

            for key in user_keys:
                if key["AccessKeyId"] == generated_key_id:
                    continue

                iam.delete_access_key(UserName=username, AccessKeyId=key["AccessKeyId"])
                keys_deleted += 1

            status = f"Removed {keys_deleted} old keys"

        click.echo(
            click.style(
                f"{repo:<15}{code:<25}{status:<15}",
                fg="green" if 200 <= code <= 299 else "red",
            )
        )

    return success


def exclude_iac_from_rotation(repos, org_name):
    warning_message = """
        This script WILL NOT rotate any secrets in the iac repository.
        If you want to rotate secrets in the iac repository, you will need to do that
        either manually, or by adjusting this script to fit the environment structure
        of the secrets in iac.
    """
    click.echo(click.style("WARNING: " + warning_message, fg="yellow"))
    repos_without_iac = []
    for repo in repos:
        if repo.full_name != f"{org_name}/iac":
            repos_without_iac.append(repo)
    return repos_without_iac


@click.command()
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
    default="biomage-org",
    help="The GitHub organization to perform the operation in.",
)
def rotate_ci(token, org):
    """
    Rotates and updates repository access credentials.
    """

    click.echo("Logging into GitHub and getting all repositories...")

    g = Github(token)
    org = g.get_organization(org)
    repos = org.get_repos()

    click.echo(
        f"Found {repos.totalCount} "
        f"repositories in organization {org.name} ({org.login}), "
        "finding ones with required CI privileges..."
    )

    repos = exclude_iac_from_rotation(repos, org.login)

    policies = [ret for ret in (filter_iam_repos(repo) for repo in repos) if ret]
    policies = dict(policies)

    click.echo(
        f"Found {len(policies.keys())} repositories marked as requiring CI IAM policies.\nThese are: {', '.join(policies.keys())}" 
    )

    questions = [
            Confirm(
                name="create",
                message="Are you sure you want to rotate ci for these repositories?",
                default=False,
            )
        ]
    click.echo()
    answer = prompt(questions, theme=GreenPassion())
    if not answer["create"]:
        exit(1)

    iam = boto3.client("iam", config=config)
    create_new_iam_users(policies)
    keys = create_new_access_keys(iam, policies)

    result_codes = update_github_secrets(keys, token, org)

    success = rollback_if_necessary(iam, keys, result_codes)

    if success:
        click.echo(click.style("✔️ All done!", fg="green", bold=True))
        exit(0)
    else:
        click.echo(
            click.style(
                "✖️ There have been errors. Check the logs and try again.",
                fg="red",
                bold=True,
            )
        )
        exit(1)
