import base64
import hashlib
import json
import math
import os
import re
from collections import namedtuple
from functools import reduce

import anybase32
import boto3
import click
import requests
import yaml
from botocore.exceptions import ClientError
from github import Github
from PyInquirer import prompt

SANDBOX_NAME_REGEX = re.compile(
    r"[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*"
)
base_branch = "develop"


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def download_templates(org, repo, ref):
    if ref:
        template = "refs-pull-{ref}-merge.yaml"
    else:
        template = f"refs-heads-{base_branch}.yaml"

    url = f"https://raw.githubusercontent.com/{org}/iac/master/releases/staging-candidates/{repo}/{template}"

    s = requests.Session()
    r = s.get(url)

    Deployment = namedtuple("Deployment", ["ref", "url", "status", "text"])

    return Deployment(
        ref=ref or base_branch, url=url, status=r.status_code, text=r.text
    )


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


def get_latest_iac_commit_sha(chart, token):
    path = chart["git"].split(":")
    org, repo = path[1].split("/")

    g = Github(token)
    org = g.get_organization(org)
    repo = org.get_repo(repo)

    for ref in repo.get_git_refs():
        if ref.ref == "refs/heads/master":
            return ref.object.sha

    raise Exception("Invalid repository supplied.")


def get_all_experiments(source_table="experiments-staging"):
    """
    Function to get all experiments from a source_table.
    This function handles pagination of results returned by DynamoDB scan.
    """
    table = boto3.resource("dynamodb").Table(source_table)
    response = table.scan(
        AttributesToGet=["experimentId", "experimentName"],
        ConsistentRead=True,
    )

    experiment_ids = response.get("Items")

    while last_key := response.get("LastEvaluatedKey"):
        response = table.scan(
            AttributesToGet=["experimentId", "experimentName"],
            ConsistentRead=True,
            ExclusiveStartKey={"experimentId": last_key.get("experimentId")},
        )
        experiment_ids = [*experiment_ids, *response.get("Items")]

    return experiment_ids


def get_sandbox_id(templates, manifests, all_experiments):
    # Generate a sandbox name and ask the user what they want theirs to be called.
    manifest_hash = hashlib.md5(manifests.encode()).digest()
    manifest_hash = anybase32.encode(manifest_hash, anybase32.ZBASE32).decode()

    pr_ids = "-".join(
        [
            f"{repo}{opts.ref}"
            for repo, opts in templates.items()
            if opts.ref != base_branch
        ]
    )

    fragments = (
        re.sub(r"[^\w\s]", "", os.getenv("BIOMAGE_NICK", os.getenv("USER", ""))),
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

        if sandbox_id in [experiment["experimentId"] for experiment in all_experiments]:
            click.echo(
                click.style(
                    "Your ID is the same with the name of an experiment. "
                    "Please use another name",
                    fg="red",
                )
            )
        elif SANDBOX_NAME_REGEX.match(sandbox_id) and len(sandbox_id) <= 26:
            return sandbox_id
        else:
            click.echo(click.style("Please, verify the syntax of your ID", fg="red"))


def create_manifest(templates, token, all_experiments):
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
        f"no new changes made to the {base_branch} branch of the `ui` repository "
        "will propagate to your sandbox after it’s created.\n"
        f"By default, only deployments sourced from the {base_branch} branch are pinned, "
        "deployments using branches you are\n"
        "likely to be testing (e.g. pull requests) are not."
    )
    questions = [
        {
            "type": "checkbox",
            "name": "pins",
            "message": "Which deployments would you like to pin?",
            "choices": [
                {"name": name, "checked": props.ref == base_branch}
                for name, props in templates.items()
            ],
        }
    ]

    click.echo()
    pins = prompt(questions)
    try:
        pins = set(pins["pins"])
        click.echo("Pinned repositories:")
        click.echo("\n".join(f"• {pin}" for pin in pins))

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
                    document["spec"]["chart"]["ref"] = get_latest_iac_commit_sha(
                        document["spec"]["chart"], token
                    )
                else:
                    document["spec"]["chart"]["ref"] = base_branch

            manifests.append(document)

    manifests = yaml.dump_all(manifests)

    # Write sandbox ID
    sandbox_id = get_sandbox_id(templates, manifests, all_experiments)
    manifests = manifests.replace("STAGING_SANDBOX_ID", sandbox_id)
    manifests = base64.b64encode(manifests.encode()).decode()

    return manifests, sandbox_id


def paginate_experiments(
    experiments,
    experiments_per_page=10,
    previous_text="<< previous <<",
    next_text=">> next >>",
    done_text="== done ==",
):
    """
    Helper function to paginate list of experiments to choose for function.
    This function returns a list of experiment ids, without the accompanying exepriment names
    """

    chosen_experiments = []

    if len(experiments) <= experiments_per_page:

        choices = [
            {
                "name": "{} {}".format(
                    experiment["experimentId"][:40].ljust(40),
                    experiment["experimentName"],
                ),
            }
            for experiment in experiments
        ]

        questions = [
            {
                "type": "checkbox",
                "name": "experiments_to_stage",
                "message": "Which experiments would you like to enable for the staging environment?",
                "choices": choices,
            }
        ]

        chosen_experiments = [
            experiment.split(" ")[0]
            for experiment in prompt(questions)["experiments_to_stage"]
        ]

    else:

        max_page = math.ceil(len(experiments) / experiments_per_page)
        current_page = 0
        done = False

        while not done:

            idx_start = experiments_per_page * current_page
            idx_end = (current_page + 1) * experiments_per_page

            choices = [
                {
                    "name": "{} {}".format(
                        experiment["experimentId"].ljust(40),
                        experiment["experimentName"],
                    ),
                    "checked": experiment["experimentId"] in chosen_experiments,
                }
                for experiment in experiments[idx_start:idx_end]
            ]

            # build choice options according to position in page
            if current_page > 0:
                choices = [{"name": previous_text}, *choices]

            if current_page < max_page - 1:
                choices = [*choices, {"name": next_text}]

            choices = [*choices, {"name": done_text}]

            questions = [
                {
                    "type": "checkbox",
                    "name": "experiments_to_stage",
                    "message": "Which experiments would you like to enable for the staging environment?\n"
                    "Choose 'done' to exit",
                    "choices": choices,
                }
            ]

            answers = prompt(questions)["experiments_to_stage"]
            page_action = None

            # Check if any of the pagination options are selected
            if previous_text in answers:
                answers.remove(previous_text)
                page_action = "previous"

            if next_text in answers:
                answers.remove(next_text)
                page_action = "next"

            if done_text in answers:
                answers.remove(done_text)
                page_action = "done"

            chosen_experiments = set(
                [
                    *chosen_experiments,
                    *[experiment.split(" ")[0] for experiment in answers],
                ]
            )

            # Switch according to chosen action
            if page_action == "next":
                current_page += 1
            elif page_action == "previous":
                current_page -= 1
            elif page_action == "done":
                break

    return chosen_experiments


def modified_records(item, target_table, config):
    """
    Return modified records.
    This function should return spreadable dictionary
    """

    if target_table == config["staging-experiments-table"]:
        return {
            # Rewrite pipeline details in experiments-table
            # metadata to pipeline ARN in production environment
            "meta": {
                "M": {
                    **item["meta"]["M"],
                    "pipeline": {
                        "M": {
                            "stateMachineArn": {"S": ""},
                            "executionArn": {"S": ""},
                        }
                    },
                }
            }
        }

    return {}


def same_object(target, source):
    """
    Check if a target object is the same with source object by comparing their etags.
    """
    same_etag = False

    try:
        s3 = boto3.client("s3")
        s3.head_object(
            Bucket=target["Bucket"], Key=target["Key"], IfMatch=source["ETag"]
        )
        same_etag = True
    except ClientError as err:
        # HTTPError 404 is returned if the object doesn't exist
        # HTTPError 412 is returned if the target's ETag doesn't match with the source's ETag
        # as a consequence of using `IfMatch` in `s3.head_object()`
        if err.response["ResponseMetadata"]["HTTPStatusCode"] not in [412, 404]:
            raise

    return same_etag


def copy_s3_files(sandbox_id, prefix, source_bucket, target_bucket):
    """
    Copy s3 files in a bucket under a prefix
    """
    s3 = boto3.client("s3")
    exp_files = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix)

    for obj in exp_files.get("Contents"):

        experiment_id = obj["Key"].split("/")[0]

        source = {"Bucket": source_bucket, "Key": obj["Key"]}

        target = {
            "Bucket": target_bucket,
            "Key": obj["Key"].replace(experiment_id, f"{sandbox_id}-{experiment_id}"),
        }

        if not same_object(target, obj):
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


def copy_dynamodb_records(
    sandbox_id, staging_experiments, source_table, target_table, config
):
    """
    Copy dynamodBD records for an experiment id
    """
    dynamodb = boto3.client("dynamodb")
    for experiment_id in staging_experiments:
        items = dynamodb.query(
            TableName=source_table,
            KeyConditionExpression="experimentId = :experiment_id",
            ExpressionAttributeValues={":experiment_id": {"S": experiment_id}},
        ).get("Items")

        items_to_insert = {
            target_table: [
                {
                    "PutRequest": {
                        "Item": {
                            **item,
                            **modified_records(item, target_table, config),
                            "experimentId": {
                                "S": f"{sandbox_id}-{item['experimentId']['S']}",
                            },
                        }
                    }
                }
                for item in items
            ]
        }

        try:
            dynamodb.batch_write_item(RequestItems=items_to_insert)
        except Exception as e:
            click.echo(f"Failed inserting records: {e}")


def select_staging_experiments(sandbox_id, all_experiments, config):
    """
    Present a dialogue to the user to select staging experiments
    """
    click.echo()
    click.echo(
        click.style("Create isolated staging environment.", fg="yellow", bold=True)
    )
    click.echo(
        "To provide isolation, files and records from existing experimentIds "
        "will be copied and renamed under unique experimentIds.\n"
        "You can use these scoped resources to test your changes. "
        "Be mindful, that creating isolated environments create resources in AWS."
    )

    staged_experiments = [
        experiment
        for experiment in all_experiments
        if re.match(f"^{sandbox_id}-*", experiment["experimentId"])
    ]

    # Exclude experiments with the pattern {sandbox_id}-{experiment_id}
    # and the associated experiment_id to prevent double staging.
    excluded_experiment_ids = [
        *[
            experiment["experimentId"].replace(f"{sandbox_id}-", "")
            for experiment in staged_experiments
        ],
        *[experiment["experimentId"] for experiment in staged_experiments],
    ]

    unstaged_experiments = [
        experiment
        for experiment in all_experiments
        if experiment["experimentId"] not in excluded_experiment_ids
    ]

    if len(staged_experiments) > 0:
        click.echo()
        click.echo(f"Staged environment(s) exists for the sandbox_id {sandbox_id}:")
        click.echo(
            "\n".join(
                [f"• {experiment['experimentId']}" for experiment in staged_experiments]
            )
        )
        click.echo()

    chosen_experiments = paginate_experiments(unstaged_experiments)

    click.echo()
    click.echo("Experiments chosen to be staged:")
    click.echo("\n".join(f"• {experiment_id}" for experiment_id in chosen_experiments))

    staged_experiment_ids = [
        experiment["experimentId"] for experiment in staged_experiments
    ]

    return chosen_experiments, staged_experiment_ids


def create_staging_experiments(staging_experiments, sandbox_id, config):
    """
    Create staging experiments from a list of experiment ids in staging_experiments
    """
    click.echo()
    click.echo("Copying items for new experiments...")

    # Copy files
    for source_bucket in config["source-buckets"]:
        target_bucket = source_bucket.replace("production", "staging")

        for experiment_id in staging_experiments:
            copy_s3_files(sandbox_id, experiment_id, source_bucket, target_bucket)

    click.echo(click.style("S3 files successfully copied.", fg="green", bold=True))
    click.echo()

    # Copy DynamoDB entries
    click.echo("Copying DynamoDB records for new experiments...")
    for source_table in config["source-tables"]:
        target_table = source_table.replace("production", "staging")

        click.echo(f"Copying records from {source_table} to table {target_table}...")
        copy_dynamodb_records(
            sandbox_id, staging_experiments, source_table, target_table, config
        )

    click.echo(
        click.style("DynamoDB records successfully copied.", fg="green", bold=True)
    )


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

    # Read configuration
    config = None
    with open("config.yaml") as config_file:
        config = list(yaml.load_all(config_file, Loader=yaml.SafeLoader))[0]

    all_experiments = get_all_experiments(config["production-experiments-table"])

    manifest, sandbox_id = create_manifest(templates, token, all_experiments)

    print("select staging experiments")
    experiment_ids_to_stage, staged_experiment_ids = select_staging_experiments(
        sandbox_id, all_experiments, config
    )

    if len(experiment_ids_to_stage) == 0:
        click.echo(
            "No experiments chosen. "
            "Skipping creation of isolated staging environment."
        )

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
            "default": False,
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

    if len(experiment_ids_to_stage) > 0:
        create_staging_experiments(experiment_ids_to_stage, sandbox_id, config)

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

    available_experiments = [*staged_experiment_ids, *experiment_ids_to_stage]

    if len(available_experiments) > 0:
        click.echo()
        click.echo(
            click.style(
                "✔️ Staging-specific experiments are available at :",
                fg="green",
                bold=True,
            )
        )

        click.echo(
            "\n".join(
                [
                    f"• https://ui-{sandbox_id}.scp-staging.biomage.net/experiments/{sandbox_id}-{experiment_id}/data-processing"
                    for experiment_id in available_experiments
                ]
            )
        )
