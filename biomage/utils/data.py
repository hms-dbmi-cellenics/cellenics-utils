import boto3
import click
from biomage.experiment.utils import (
    add_env_user_to_experiment,
    get_experiment_project_id,
)
from botocore.exceptions import ClientError

from ..utils.constants import PRODUCTION, STAGING


def modify_records(item, target_table, config, **extra):
    """
    Return modified records.
    This function should return spreadable dictionary
    """

    if target_table == config["staging-experiments-table"]:

        item["projectId"]["S"] = f"{extra['sandbox_id']}-{item['projectId']['S']}"
        item = add_env_user_to_experiment(cfg=item)

        return item

    if target_table == config["staging-samples-table"]:
        return {
            "projectUuid": {"S": f"{extra['sandbox_id']}-{item['projectUuid']['S']}"},
        }

    if target_table == config["staging-projects-table"]:

        new_experiments_list = []
        for experiment_id in item["projects"]["M"]["experiments"]["L"]:
            new_experiments_list.append(
                {"S": f"{extra['sandbox_id']}-{experiment_id['S']}"}
            )

        item["projects"]["M"]["experiments"]["L"] = new_experiments_list

        item["projects"]["M"]["uuid"][
            "S"
        ] = f"{extra['sandbox_id']}-{item['projectUuid']['S']}"

        return item

    return {}


def definitely_equal(target, source):
    """
    Returns if 2 objects are equal. Only positive return values are reliable. Two
    objects might be equal and return false due to a number of reasons like:
    * We can't reliably use etags for object comparison
    * If there's any exception trying to get the target bucket, we'll just return false.

    The method is only useful to avoid copying again objects that are definitely
    equal.
    """
    same_etag = False

    try:
        s3 = boto3.client("s3")
        s3.head_object(
            Bucket=target["Bucket"], Key=target["Key"], IfMatch=source["ETag"]
        )
        same_etag = True
    except ClientError:
        # if there's any exception assume the comparison failed a return false
        #  (which can be a false negative or a true negative)
        pass

    return same_etag


def copy_s3_files(sandbox_id, prefix, source_bucket, target_bucket):
    """
    Copy s3 files in a bucket under a prefix
    """
    s3 = boto3.client("s3")
    exp_files = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix)

    if "Contents" not in exp_files:
        raise Exception(
            f"Failed to do an experiment copy: bucket {source_bucket} doesn't contain {prefix} as prefix."
        )

    for obj in exp_files.get("Contents"):

        experiment_id = obj["Key"].split("/")[0]
        target_key = obj["Key"].replace(experiment_id, f"{sandbox_id}-{experiment_id}")

        source = {"Bucket": source_bucket, "Key": obj["Key"]}

        target = {
            "Bucket": target_bucket,
            "Key": target_key,
        }

        if not definitely_equal(target, obj):
            click.echo(
                f"Copying from {source['Bucket']}/{source['Key']} to "
                f"{target['Bucket']}/{target['Key']}"
            )
            try:
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

    if "projects-" in source_table:
        copy_project_record(
            sandbox_id, staging_experiments, source_table, target_table, config
        )
        return

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
                            **modify_records(
                                item, target_table, config, sandbox_id=sandbox_id
                            ),
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


def copy_project_record(
    sandbox_id, staging_experiments, source_table, target_table, config
):

    dynamodb = boto3.client("dynamodb")
    for experiment_id in staging_experiments:

        project_id = get_experiment_project_id(
            experiment_id, config["production-experiments-table"]
        )

        item = dynamodb.get_item(
            TableName=source_table, Key={"projectUuid": {"S": project_id}}
        ).get("Item")

        dynamodb.put_item(
            TableName=target_table,
            Item={
                **modify_records(item, target_table, config, sandbox_id=sandbox_id),
                "projectUuid": {"S": f"{sandbox_id}-{project_id}"},
            },
        )


def copy_experiments_to(
    experiments, prefix, config, origin=PRODUCTION, destination=STAGING
):
    """
    Copy the list of experiment IDs in experiments from the origin env into
    destination env.
    """
    click.echo()
    click.echo("Copying items for new experiments...")

    buckets = config["source-buckets"]
    # Copy files
    for source_bucket in buckets:
        target_bucket = source_bucket.replace(origin, destination)

        for experiment_id in experiments:
            if "biomage-originals-" in target_bucket:
                project_id = get_experiment_project_id(
                    experiment_id, config["production-experiments-table"]
                )
                copy_s3_files(prefix, project_id, source_bucket, target_bucket)
                continue

            copy_s3_files(prefix, experiment_id, source_bucket, target_bucket)

    click.echo(click.style("S3 files successfully copied.", fg="green", bold=True))
    click.echo()

    # Copy DynamoDB entries
    click.echo("Copying DynamoDB records for new experiments...")
    for source_table in config["source-tables"]:
        target_table = source_table.replace(origin, destination)

        click.echo(f"Copying records from {source_table} to table {target_table}...")
        copy_dynamodb_records(prefix, experiments, source_table, target_table, config)

    click.echo(
        click.style("DynamoDB records successfully copied.", fg="green", bold=True)
    )
