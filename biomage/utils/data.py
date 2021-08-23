import boto3
import click
from biomage.experiment.utils import (
    add_env_user_to_experiment,
    get_experiment_project_id,
)
from botocore.exceptions import ClientError

from ..utils.constants import PRODUCTION, STAGING


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


def copy_s3_bucket(prefix, source_bucket, target_bucket):
    """
    Copy s3 files in a bucket under a prefix. Prefix is normally an experiment or
     project id
    """
    s3 = boto3.client("s3")
    exp_files = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix)

    if "Contents" not in exp_files:
        raise Exception(
            f"Failed to do an experiment copy: bucket {source_bucket} doesn't contain {prefix} as prefix."
        )

    for obj in exp_files.get("Contents"):

        target_key = obj["Key"]

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


def copy_s3_data(experiments, config, origin, destination):
    buckets = config["source-buckets"]
    for experiment_id in experiments:

        for source_bucket in buckets:
            target_bucket = source_bucket.replace(origin, destination)

            if "biomage-originals-" in target_bucket:
                project_id = get_experiment_project_id(
                    experiment_id, config["production-experiments-table"]
                )
                copy_s3_bucket(project_id, source_bucket, target_bucket)
                continue

            copy_s3_bucket(experiment_id, source_bucket, target_bucket)


def copy_dynamodb_records(experiment_id, source_table, target_table, config):
    """
    Copy dynamodBD records for an experiment id
    """

    # projects table uses project ID as PK so it's copied a bit differently
    if "projects-" in source_table:
        copy_project_record(experiment_id, source_table, target_table, config)
        return

    dynamodb = boto3.client("dynamodb")
    items = dynamodb.query(
        TableName=source_table,
        KeyConditionExpression="experimentId = :experiment_id",
        ExpressionAttributeValues={":experiment_id": {"S": experiment_id}},
    ).get("Items")

    items_to_insert = []
    for item in items:
        item = add_env_user_to_experiment(cfg=item)
        items_to_insert.append({"PutRequest": {"Item": item}})

    insert_request = {target_table: items_to_insert}

    try:
        dynamodb.batch_write_item(RequestItems=insert_request)
    except Exception as e:
        click.echo(f"Failed inserting records: {e}")


def copy_project_record(experiment_id, source_table, target_table, config):

    dynamodb = boto3.client("dynamodb")

    project_id = get_experiment_project_id(
        experiment_id, config["production-experiments-table"]
    )

    item = dynamodb.get_item(
        TableName=source_table, Key={"projectUuid": {"S": project_id}}
    ).get("Item")

    dynamodb.put_item(TableName=target_table, Item=item)


def copy_dynamodb_data(experiments, config, origin, destination):
    for experiment_id in experiments:
        for source_table in config["source-tables"]:
            target_table = source_table.replace(origin, destination)

            click.echo(
                f"Copying records from {source_table} to table {target_table}..."
            )
            copy_dynamodb_records(experiment_id, source_table, target_table, config)


def copy_experiments_to(experiments, config, origin=PRODUCTION, destination=STAGING):
    """
    Copy the list of experiment IDs in experiments from the origin env into
    destination env.
    """
    click.echo()
    click.echo("Copying items for new experiments...")

    copy_s3_data(
        experiments=experiments, config=config, origin=origin, destination=destination
    )
    click.echo(click.style("S3 files successfully copied.", fg="green", bold=True))
    click.echo()

    # Copy DynamoDB entries
    click.echo("Copying DynamoDB records for new experiments...")
    copy_dynamodb_data(
        experiments=experiments, config=config, origin=origin, destination=destination
    )
    click.echo(
        click.style("DynamoDB records successfully copied.", fg="green", bold=True)
    )
