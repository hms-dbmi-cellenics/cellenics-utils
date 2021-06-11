import copy
import itertools
from pprint import pprint

import boto3
import click
from botocore.exceptions import ClientError
from deepdiff import DeepDiff
from PyInquirer import prompt

from ..utils.constants import DEVELOPMENT, PRODUCTION, STAGING

from .copy import copy
from .ls import ls
from .pull import pull


@click.group()
def experiment():
    """
    Manage  Cellscope experiment data and settings.
    """
    pass


def hash_cell_ids(ids):
    return f"Hashed_as_{abs(hash(tuple(ids)))}"


def experiment_record(db_table, experimentId):
    record = db_table.get_item(Key={"experimentId": experimentId})
    if "Item" in record:
        return record["Item"]


def experiment_item_summary(item, env_name):
    CELL_SETS = "cellSets"
    CELL_IDS = "cellIds"
    CHILDREN = "children"
    summary = copy.deepcopy(item)
    if CELL_SETS in summary:
        for cellSet in summary[CELL_SETS]:
            if CELL_IDS in cellSet:
                cellSet[CELL_IDS] = hash_cell_ids(cellSet[CELL_IDS])
            if CHILDREN in cellSet:
                for child in cellSet[CHILDREN]:
                    child[CELL_IDS] = hash_cell_ids(child[CELL_IDS])
    MATRIX_PATH = "matrixPath"
    if MATRIX_PATH in summary:
        summary[MATRIX_PATH] = summary[MATRIX_PATH].replace(env_name, "${ENV}")
    return summary


@click.command()
@click.argument("experiment_id")
def compare(experiment_id):
    """
    Compares an experiment's information accross environments
    """

    environments = {
        DEVELOPMENT: {
            "endpoint_url": "http://localhost:4566",
            "s3": {},
            "dynamoDB": {},
        },
        STAGING: {
            "s3": {},
            "dynamoDB": {},
        },
        PRODUCTION: {
            "s3": {},
            "dynamoDB": {},
        },
    }
    tables = {
        "experiments": {"query": experiment_record, "summary": experiment_item_summary},
        # "plots-tables" is not very interesting unless we can access the s3 buckets
        # that contain the actual plot data. I lack privileges and access them.
    }
    buckets = {
        "biomage-source": {},
    }

    for env_name, env_values in environments.items():
        dynamodb = boto3.resource(
            "dynamodb", endpoint_url=env_values.get("endpoint_url")
        )
        for table_name, table in tables.items():
            db_table = dynamodb.Table(f"{table_name}-{env_name}")
            try:
                record = table["query"](db_table, experiment_id)
                if record:
                    env_values["dynamoDB"][table_name] = table["summary"](
                        record, env_name
                    )

            except ClientError as e:
                click.echo(e.response["Error"]["Message"])

        s3 = boto3.client("s3", endpoint_url=env_values.get("endpoint_url"))
        for bucket_name in buckets.keys():
            env_values["s3"][bucket_name] = {}
            for s3_object in s3.list_objects(
                Bucket=f"{bucket_name}-{env_name}", Prefix=experiment_id
            ).get("Contents", []):
                file_name = s3_object["Key"].split("/", 1)[1]
                env_values["s3"][bucket_name][file_name] = {
                    "ETag": s3_object["ETag"],
                    "Size": s3_object["Size"],
                    "LastModified": s3_object["LastModified"].isoformat(),
                }

    diff_details = []
    for table_name in tables.keys():
        click.echo(f"Comparing records for table {table_name}")
        missing = [
            env_name
            for env_name in environments.keys()
            if table_name not in environments[env_name]["dynamoDB"]
        ]
        available = [
            env_name for env_name in environments.keys() if env_name not in missing
        ]
        if len(missing):
            click.echo(
                click.style(
                    f"✖ No record for {table_name} in {missing}", fg="white", bg="red"
                )
            )
        if len(available) >= 2:
            for env1, env2 in itertools.combinations(available, 2):
                diff = DeepDiff(
                    environments[env1]["dynamoDB"][table_name],
                    environments[env2]["dynamoDB"][table_name],
                )
                if not diff:
                    click.echo(
                        click.style(
                            f"✔️ {env1} and {env2} are equal for table '{table_name}'",
                            bg="green",
                        )
                    )
                else:
                    click.echo(
                        click.style(
                            f"✖ {env1} and {env2} differ for table '{table_name}'",
                            fg="white",
                            bg="red",
                        )
                    )
                    diff_details.append(("table", table_name, env1, env2, diff))

    for bucket_name in buckets.keys():
        click.echo(f"Comparing files for bucket {bucket_name}")
        missing = [
            env_name
            for env_name in environments.keys()
            if bucket_name not in environments[env_name]["s3"]
        ]
        available = [
            env_name for env_name in environments.keys() if env_name not in missing
        ]
        if len(missing):
            click.echo(
                click.style(
                    f"✖ No files on {bucket_name} in {missing}", fg="white", bg="red"
                )
            )
        if len(available) >= 2:
            for env1, env2 in itertools.combinations(available, 2):
                # this relies on this script changing the local file modified date to
                # that of its remote counterpart, otherwise it will always fail as the
                # S3 modification date (upload time) will always differ from the local
                # file modified date (download time)
                diff = DeepDiff(
                    environments[env1]["s3"][bucket_name],
                    environments[env2]["s3"][bucket_name],
                )
                if not diff:
                    click.echo(
                        click.style(
                            f"✔️ {env1} and {env2} are equal for bucket '{bucket_name}'",
                            bg="green",
                        )
                    )
                else:
                    click.echo(
                        click.style(
                            f"✖ {env1} and {env2} differ for bucket '{bucket_name}'",
                            fg="white",
                            bg="red",
                        )
                    )
                    diff_details.append(("bucket", bucket_name, env1, env2, diff))

    if len(diff_details):
        questions = [
            {
                "type": "confirm",
                "name": "details",
                "default": False,
                "message": "Do you want and know more details about the differences?",
            }
        ]
        answers = prompt(questions)
        if answers["details"]:
            for setting_type, setting_name, env1, env2, diff in diff_details:
                questions = [
                    {
                        "type": "confirm",
                        "name": "details",
                        "message": f"More details about {env1} vs {env2} regarding "
                        f"{setting_type} '{setting_name}'?",
                    }
                ]
                answers = prompt(questions)
                if answers["details"]:
                    click.echo(
                        click.style(
                            f"Comparing {env1} and {env2} for {setting_type} "
                            f"'{setting_name}'",
                            fg="white",
                            bg="red",
                        )
                    )
                    pprint(diff)


experiment.add_command(compare)
experiment.add_command(pull)
experiment.add_command(ls)
experiment.add_command(copy)
