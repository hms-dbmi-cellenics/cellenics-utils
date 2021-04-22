import click
import requests
import json
import yaml
import hashlib
import anybase32
import base64
import boto3
import re
import os
from collections import namedtuple
from PyInquirer import prompt
from github import Github
from functools import reduce

SANDBOX_NAME_REGEX = re.compile(
    r"[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*"
)


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def download_templates(org, repo, ref):
    if not ref:
        url = f"https://raw.githubusercontent.com/{org}/iac/master/releases/staging-candidates/{repo}/refs-heads-master.yaml"  # noqa: E501
    else:
        url = f"https://raw.githubusercontent.com/{org}/iac/master/releases/staging-candidates/{repo}/refs-pull-{ref}-merge.yaml"  # noqa: E501

    s = requests.Session()
    r = s.get(url)

    Deployment = namedtuple("Deployment", ["ref", "url", "status", "text"])

    return Deployment(ref=ref or "master", url=url, status=r.status_code, text=r.text)


def compile_requirements(org, deployments):
    REPOS = ("ui", "api", "worker", "pipeline")

    repo_to_ref = {deployment: None for deployment in REPOS}

    for deployment in deployments:
        try:
            repo, pr_id = deployment.split("/", 1)
            repo_to_ref[repo] = int(pr_id)
        except Exception:
            repo_to_ref[deployment] = None

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


def get_sandbox_id(templates, manifests):
    # Generate a sandbox name and ask the user what they want theirs to be called.
    manifest_hash = hashlib.md5(manifests.encode()).digest()
    manifest_hash = anybase32.encode(manifest_hash, anybase32.ZBASE32).decode()
    pr_ids = "-".join(
        [
            f"{repo}{opts.ref}"
            for repo, opts in templates.items()
            if opts.ref != "master"
        ]
    )

    fragments = (
        os.getenv("BIOMAGE_NICK", os.getenv("USER", "")),
        pr_ids if pr_ids else manifest_hash,
    )
    sandbox_id = "-".join([bit for bit in fragments if bit]).lower()[:26]

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
    while True:
        questions = [
            {
                "type": "input",
                "name": "sandbox_id",
                "message": "Provide an ID:",
                "default": sandbox_id,
            }
        ]

        click.echo()
        sandbox_id = prompt(questions)
        sandbox_id = sandbox_id["sandbox_id"]
        if SANDBOX_NAME_REGEX.match(sandbox_id) and len(sandbox_id) <= 26:
            return sandbox_id
        else:
            click.echo(click.style("Please, verify the syntax of your ID", fg="red"))


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
        "The sandbox will not be affected by any future changes made to pinned "
        "deployments. For example, if you pin `ui`,\n"
        "no new changes made to the `master` branch of the `ui` repository "
        "will propagate to your sandbox after it’s created.\n"
        "By default, only deployments sourced from the `master` branch are pinned, "
        "deployments using branches you are\n"
        "likely to be testing (e.g. pull requests) are not."
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
            if recursive_get(document, "spec", "chart", "ref"):
                if name in pins:
                    document["spec"]["chart"]["ref"] = get_latest_master_sha(
                        document["spec"]["chart"], token
                    )
                else:
                    document["spec"]["chart"]["ref"] = "master"

            manifests.append(document)

    manifests = yaml.dump_all(manifests)

    # Write sandbox ID
    sandbox_id = get_sandbox_id(templates, manifests)
    manifests = manifests.replace("STAGING_SANDBOX_ID", sandbox_id)

    return manifests, sandbox_id


def choose_staging_experiments():
    # Get list of experiments currently available in the platform
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("experiments-staging")
    response = table.scan(
        AttributesToGet=["experimentId", "experimentName"],
        Limit=20,
        ConsistentRead=True,
    )

    # Implement pagination if result contains more than 20 experiments
    default_enabled_experiments = ["e52b39624588791a7889e39c617f669e"]

    choices = [
        {
            "name": "{}{}".format(
                props.get("experimentId").ljust(36), props.get("experimentName")
            ),
            "checked": props.get("experimentId") in default_enabled_experiments,
        }
        for props in response.get("Items")
    ]

    click.echo()
    click.echo(click.style("Isolate staging environment.", fg="yellow", bold=True))
    click.echo(
        "To provide isolation, files and records from existing experimentIds "
        "will be copied and renamed under unique experimentIds.\nYou can use these "
        "scoped resources to test your changes. Be mindful, that creating\n"
        "isolated environmetns involve copying resources in AWS."
    )
    questions = [
        {
            "type": "checkbox",
            "name": "staging_experiments",
            "message": "Which experiments would you like to enable for the staging environment?",
            "choices": choices,
        }
    ]
    click.echo()

    answers = prompt(questions)

    try:
        staging_experiments = set(
            [
                experiment_id.split(" ")[0]
                for experiment_id in answers["staging_experiments"]
            ]
        )
    except Exception:
        exit(1)

    return staging_experiments


def create_staging_experiments(staging_experiments, sandbox_id):
    # Create staging experiments as selected

    click.echo()
    click.echo("Copying items for new experiments...")

    # Copy files
    s3 = boto3.client("s3")

    source_buckets = [
        "processed-matrix-staging",
        "biomage-source-staging",
    ]

    file_copy_retries = []

    for bucket in source_buckets:
        for experiment_id in staging_experiments:
            exp_files = s3.list_objects_v2(Bucket=bucket, Prefix=experiment_id)

            if exp_files.get("KeyCount") == 0:
                click.echo(f"No objects found in {bucket}, skipping bucket")
                continue

            for obj in exp_files.get("Contents"):

                experiment_id = obj["Key"].split("/")[0]

                source = {"Bucket": bucket, "Key": obj["Key"]}

                target = {
                    "Bucket": bucket,
                    "Key": obj["Key"].replace(
                        experiment_id, f"{sandbox_id}-{experiment_id}"
                    ),
                }

                try:
                    # Skip copying if object exists
                    s3.head_object(
                        Bucket=bucket, Key=target["Key"], IfMatch=obj["ETag"]
                    )

                except Exception:

                    try:
                        click.echo(
                            f"Copying from {source['Bucket']}/{source['Key']} to "
                            f"{target['Bucket']}/{target['Key']}"
                        )

                        s3.copy_object(
                            CopySource=source,
                            Bucket=target["Bucket"],
                            Key=target["Key"],
                        )
                    except Exception as e:
                        click.echo(
                            f"failed to copy object {source['Bucket']}/{source['Key']} \
                            with exception: \n {e}"
                        )
                        file_copy_retries.append(source)

    click.echo(click.style("S3 files successfully copied.", fg="green", bold=True))
    click.echo()

    # Copy DynamoDB entries
    dynamodb = boto3.client("dynamodb")
    source_tables = ["experiments-staging", "samples-staging"]

    click.echo("Copying DynamoDB records for new experiments...")

    request_items = {}

    for table in source_tables:
        click.echo(f"Copying records in {table}")

        for experiment_id in staging_experiments:
            items = dynamodb.query(
                TableName=table,
                KeyConditionExpression="experimentId = :experiment_id",
                ExpressionAttributeValues={":experiment_id": {"S": experiment_id}},
            ).get("Items")

            items_to_insert = {}
            items_to_insert[table] = [
                {
                    "PutRequest": {
                        "Item": {
                            **item,
                            "experimentId": {
                                "S": f"{sandbox_id}-{item['experimentId']['S']}",
                            },
                        }
                    }
                }
                for item in items
            ]

            try:
                dynamodb.batch_write_item(RequestItems=items_to_insert)

            except Exception as e:
                click.echo(f"Failed inserting records: {e}")

    click.echo(
        click.style("DynamoDB records successfully copied.", fg="green", bold=True)
    )
    click.echo()


@click.command()
@click.argument("deployments", nargs=-1)
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
def stage(token, org, deployments):
    """
    Deploys a custom staging environment.
    """

    # generate templats
    templates = compile_requirements(org, deployments)
    manifest, sandbox_id = create_manifest(templates, token)
    manifest = base64.b64encode(manifest.encode()).decode()

    # enable experiments in staging
    staging_experiments = choose_staging_experiments()

    # Creating staging experiments
    create_staging_experiments(staging_experiments, sandbox_id)

    exit(0)

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

    click.echo(
        click.style(
            "Staging-specific experiments are available at :",
            fg="yellow",
            bold=True,
        )
    )

    click.echo(
        click.style(
            "\n".join(
                [
                    "Staging-scoped experiments are available at "
                    f"https://ui-{sandbox_id}.scp-staging.biomage.net/experiments/{experiment_id}/data-processing"
                    for experiment_id in staging_experiments
                ]
            )
        )
    )
