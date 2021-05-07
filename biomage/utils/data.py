import boto3
import click
from botocore.exceptions import ClientError


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
    NOTE: two objects might still be equal and have different etag
    The method is useful to avoid copying again objects that are definitely
    equal.
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


def copy_experiments_to(
    experiments, sandbox_id, config, origin="production", destination="staging"
):
    """
    Copy the list of experiment IDs in experiments from the origin env into destination env.
    """
    click.echo()
    click.echo("Copying items for new experiments...")

    buckets = config["source-buckets"]
    # Copy files
    for source_bucket in buckets:
        target_bucket = source_bucket.replace(origin, destination)

        for experiment_id in experiments:
            copy_s3_files(sandbox_id, experiment_id, source_bucket, target_bucket)

    click.echo(click.style("S3 files successfully copied.", fg="green", bold=True))
    click.echo()

    # Copy DynamoDB entries
    click.echo("Copying DynamoDB records for new experiments...")
    for source_table in config["source-tables"]:
        target_table = source_table.replace(origin, destination)

        click.echo(f"Copying records from {source_table} to table {target_table}...")
        copy_dynamodb_records(
            sandbox_id, experiments, source_table, target_table, config
        )

    click.echo(
        click.style("DynamoDB records successfully copied.", fg="green", bold=True)
    )
