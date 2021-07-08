import base64
import json
import re

import boto3
import click
import requests
from github import Github
from PyInquirer import prompt

from ..utils.config import get_config


def check_if_sandbox_exists(org, sandbox_id):
    url = f"https://raw.githubusercontent.com/{org}/iac/master/releases/staging/{sandbox_id}.yaml"  # noqa: E501

    s = requests.Session()
    r = s.get(url)

    return 200 <= r.status_code < 300


def delete_staging_records(sandbox_id, config):
    """
    Deletes resources staged under sandbox_id
    """

    # Get all experiments under a specific staging ID
    # Does not handle DynamoDB scan pagination, so result is
    # limited to 20 experiments prefixed with 'sandbox_id'
    click.echo("Removing staging records from DynamoDB")
    dynamodb = boto3.client("dynamodb")
    staged_experiments = dynamodb.scan(
        TableName=config["staging-experiments-table"],
        ProjectionExpression="experimentId",
        FilterExpression="begins_with(experimentId, :sandbox_id)",
        ExpressionAttributeValues={":sandbox_id": {"S": sandbox_id}},
    )

    staged_experiments = [
        experiment_id["experimentId"]["S"]
        for experiment_id in staged_experiments.get("Items")
    ]

    if len(staged_experiments) == 0:
        click.echo(
            "No scoped experiments registered in table"
            f" {config['staging-experiments-table']}"
        )
        click.echo()
        return

    all_staging_tables = [
        table
        for table in dynamodb.list_tables().get("TableNames")
        if re.match(".*-staging", table)
    ]

    for table in all_staging_tables:

        # Deleting records from DynamoDB requires the primaryKey.
        # if the table's PK is made up of partitionKey & sortKey,
        # will have to iterate over all the sortKeys to add as delete parameter
        key_schema = (
            dynamodb.describe_table(TableName=table).get("Table").get("KeySchema")
        )

        if len(key_schema) > 1:
            sort_key = key_schema[1].get("AttributeName")
            for experiment_id in staged_experiments:

                # Query table for all keys
                items_to_delete = dynamodb.query(
                    TableName=table,
                    ProjectionExpression=f"experimentId, {sort_key}",
                    KeyConditionExpression="experimentId = :experiment_id",
                    ExpressionAttributeValues={":experiment_id": {"S": experiment_id}},
                ).get("Items")

                if len(items_to_delete) == 0:
                    click.echo(
                        f"No records to delete in table {table}, skipping table..."
                    )
                    break

                for item in items_to_delete:
                    try:

                        key = {
                            "experimentId": {
                                "S": experiment_id,
                            },
                            sort_key: item[sort_key],
                        }

                        dynamodb.delete_item(
                            TableName=table,
                            Key=key,
                        )
                    except Exception as e:
                        click.echo(
                            f"Failed to delete record {key} from table {table}: {e}"
                        )

        else:
            for experiment_id in staged_experiments:

                key = {"experimentId": {"S": experiment_id}}

                try:
                    dynamodb.delete_item(
                        TableName=table,
                        Key=key,
                    )
                except Exception as e:
                    click.echo(f"Failed to delete record {key} from table {table}: {e}")

    click.echo(
        click.style(
            "Staging records successfully deleted from DynamoDB.", fg="green", bold=True
        )
    )
    click.echo()


def delete_staging_files(sandbox_id, config):
    """
    Remove files staged prefixed by sandbox_id
    """

    click.echo("Removing staging files from S3")
    s3 = boto3.client("s3")

    all_staging_buckets = [
        name["Name"]
        for name in s3.list_buckets().get("Buckets")
        if re.match(".*-staging", name["Name"])
    ]

    for bucket in all_staging_buckets:

        files_to_delete = s3.list_objects_v2(Bucket=bucket, Prefix=f"{sandbox_id}-")

        if files_to_delete.get("KeyCount") == 0:
            click.echo(f"No files in bucket {bucket}, skipping bucket...")
            continue

        files_to_delete = [
            obj["Key"]
            for obj in files_to_delete.get("Contents")
            if re.match(sandbox_id, obj["Key"])
        ]

        if len(files_to_delete) == 0:
            click.echo(f"No files to delete in bucket {bucket}, skipping bucket...")
            continue

        try:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj_key} for obj_key in files_to_delete]},
            )

            click.echo(
                "\n".join(
                    [f"{bucket}/{filename} deleted" for filename in files_to_delete]
                )
            )
        except Exception as e:
            click.echo("Failed to delete files" "\n".join(files_to_delete))
            click.echo(f"from {bucket} with exception : {e}")

    click.echo(
        click.style(
            "Staging files successfully deleted from S3.", fg="green", bold=True
        )
    )
    click.echo()


def remove_staging_resources(sandbox_id, config):
    """
    Remove staged resources under sandbox_id
    """
    click.echo("Deleting resources used in staging environment...")
    delete_staging_records(sandbox_id, config)
    delete_staging_files(sandbox_id, config)

    click.echo(
        click.style("Staging environments successfully removed.", fg="green", bold=True)
    )
    click.echo()


@click.command()
@click.argument("sandbox_id", nargs=1)
@click.option(
    "--token",
    "-t",
    envvar="GITHUB_API_TOKEN",
    required=True,
    show_default=True,
    help="A GitHub Personal Access Token with the required permissions.",
)
@click.option(
    "--org",
    envvar="GITHUB_BIOMAGE_ORG",
    default="biomage-ltd",
    show_default=True,
    help="The GitHub organization to perform the operation in.",
)
def unstage(token, org, sandbox_id):
    """
    Removes a custom staging environment.
    """

    # Read configuration
    config = get_config()

    if check_if_sandbox_exists(org, sandbox_id):
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
                f"with ID `{sandbox_id}`. This cannot be undone.",
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
            inputs={"sandbox-id": sandbox_id, "secrets": secrets},
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

    else:
        click.echo()
        click.echo(
            click.style(
                f"Staging sandbox with ID `{sandbox_id}` could not be found."
                "Proceeding with the removal of staged resources.",
                fg="yellow",
                bold=True,
            )
        )

    click.echo()
    remove_staging_resources(sandbox_id, config)
