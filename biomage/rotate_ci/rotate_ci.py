import click
from functools import reduce
from botocore.config import Config
import boto3
from github import Github
import json
import requests
from base64 import b64encode
from nacl import encoding, public
import cfn_flip
import time

config = Config(
    region_name="eu-west-1",
)


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def encrypt(public_key, secret_value):
    public_key = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
    sealed_box = public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return b64encode(encrypted).decode("utf-8")


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


def create_new_iam_users(iam, policies):
    users = {}

    for repo, policies in policies.items():
        users[f"{repo.capitalize()}CIUser"] = {
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
            stack = cf.update_stack(**kwargs)
        elif "No updates are to be performed" in str(e):
            click.echo("All users are up to date.")
            return
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

        r = s.put(
            f"{url_base}/{repo_name}/actions/secrets/AWS_ACCESS_KEY_ID",
            json={"encrypted_value": access_key_id, "key_id": ci_keys["key_id"]},
        )

        r = s.put(
            f"{url_base}/{repo_name}/actions/secrets/AWS_SECRET_ACCESS_KEY",
            json={"encrypted_value": secret_access_key, "key_id": ci_keys["key_id"]},
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
    default="biomage-ltd",
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

    policies = [ret for ret in (filter_iam_repos(repo) for repo in repos) if ret]
    click.echo(
        f"Found {len(policies)} repositories marked as requiring CI IAM policies."
    )
    policies = dict(policies)

    iam = boto3.client("iam", config=config)
    create_new_iam_users(iam, policies)
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
