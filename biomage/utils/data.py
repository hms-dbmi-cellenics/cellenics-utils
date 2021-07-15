import sys

import boto3
import click
from biomage.experiment.utils import add_env_user_to_experiment
from botocore.exceptions import ClientError

from ..utils.constants import PRODUCTION, STAGING


def remap_sample_references(samples, sandbox_id):
    """
    Edit entries in samples dictionary to opoint to the right prefixed resources
    """

    remapped_samples = {"M" : {}}

    for sample_id in samples['M']:

        prefixed_sample_name = f"{sandbox_id}-{sample_id}"
        prefixed_project_uuid = f"{sandbox_id}-{samples['M'][sample_id]['M']['projectUuid']['S']}"

        remapped_samples['M'][prefixed_sample_name] = {
                "M": {
                    **samples['M'][sample_id]['M'],
                    "files": remap_file_references(
                        samples['M'][sample_id]["M"]["files"],
                        sandbox_id
                    ),
                    "uuid": {"S" : prefixed_sample_name},
                    "projectUuid": {
                        "S" : prefixed_project_uuid
                    }
                }
        }

    return remapped_samples


def remap_file_references(files, sandbox_id):
    """
    Edit entries in files dictionary to opoint to the right prefixed resources
    """

    valid_filenames = [file for file in files["M"] if file != "lastModified"]

    remapped_files = {"M" : {}}
    
    for file in valid_filenames:

        remapped_files['M'][file] = {
                "M": {
                    **files['M'][file]['M'],
                    "path": {
                        "S": f"{sandbox_id}-{files['M'][file]['M']['path']['S']}"
                    },
                    "lastModified": files['M'][file]['M']['lastModified']
                },
        }

    remapped_files["M"]["lastModified"] = files['M']['lastModified']

    return remapped_files


def modify_records(item, target_table, config, **extra):
    """
    Return modified records.
    This function should return spreadable dictionary
    """

    if target_table == config["staging-experiments-table"]:
        return {
            # Modify project id to point to sandboxed project
            "projectId": {
                'S': f"{extra['sandbox_id']}-{item['projectId']['S']}",
            },
            # Add the current user's id to experiment
            **add_env_user_to_experiment(cfg={
                "rbac_can_write": {
                    "SS": item['rbac_can_write']['SS']
                }
            })
        }

    if target_table == config["staging-samples-table"]:
        return {
            "projectUuid": {"S" : f"{extra['sandbox_id']}-{item['projectUuid']['S']}"},
            "samples": remap_sample_references(item['samples'], extra['sandbox_id'])
        }

    if target_table == config["staging-projects-table"]:
        return {
            "projects" : {
                "M" : {
                    **item["projects"]["M"],
                    # Add sandbox_id to existing experiments
                    "experiments": {
                        "L" : [
                            {"S" : f"{extra['sandbox_id']}-{experiment_id['S']}"}
                            for experiment_id in item["projects"]["M"]["experiments"]["L"]
                        ]
                    },
                    # Add sandbox_id to project uuid
                    "uuid": {
                        "S": f"{extra['sandbox_id']}-{item['projectUuid']['S']}"
                    },
                    # Add sandbox_id to existing samples
                    "samples": {
                        "L" : [
                            {"S" : f"{extra['sandbox_id']}-{samples_id['S']}"}
                            for samples_id in item["projects"]["M"]["samples"]["L"]
                        ]
                    }

                }
            }
        }

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

    for obj in exp_files.get("Contents"):

        experiment_id = obj["Key"].split("/")[0]

        source = {"Bucket": source_bucket, "Key": obj["Key"]}

        target = {
            "Bucket": target_bucket,
            "Key": obj["Key"].replace(experiment_id, f"{sandbox_id}-{experiment_id}"),
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

    if 'projects-' in source_table:
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
                                item,
                                target_table,
                                config,
                                sandbox_id=sandbox_id
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
        project_id = dynamodb.get_item(
            TableName="experiments-production",
            Key={"experimentId": {"S": experiment_id}}
        ).get("Item")['projectId']['S']

        item = dynamodb.get_item(
            TableName=source_table,
            Key={"projectUuid": {'S' : project_id}}
        ).get("Item")

        try:
            dynamodb.put_item(
                TableName=target_table,
                Item={
                    **item,
                    "projectUuid": {
                        "S": f"{sandbox_id}-{project_id}"
                    },
                    **modify_records(
                        item,
                        target_table,
                        config,
                        sandbox_id=sandbox_id
                    ),
                },
            )
        except Exception as e:
            click.echo(f"Failed inserting project {project_id} with error: {e}")


def copy_experiments_to(
    experiments, sandbox_id, config, origin=PRODUCTION, destination=STAGING
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
