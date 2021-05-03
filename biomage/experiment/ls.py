import concurrent.futures
import itertools

import boto3
import click


def get_bucket_files(bucket):
    s3 = boto3.client("s3")  # low-level functional API
    return s3.list_objects(Bucket=bucket).get("Contents", [])


def list_bucket_files(bucket):
    objects = get_bucket_files(bucket)
    print(f"Bucket {bucket} files")
    print("".join([" * %s\n" % f["Key"] for f in objects]))


def get_table_items(dynamo_client, *, table_name, **kwargs):
    """
    Generates all the items in a DynamoDB table.

    :param dynamo_client: A boto3 client for DynamoDB.
    :param table_name: The name of the table to scan.

    Other keyword arguments will be passed directly to the Scan operation.
    See
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan

    This does a Parallel Scan operation over the table.

    """
    # How many segments to divide the table into?  As long as this is >= to the
    # number of threads used by the ThreadPoolExecutor, the exact number doesn't
    # seem to matter.
    total_segments = 25

    # How many scans to run in parallel?  If you set this really high you could
    # overwhelm the table read capacity, but otherwise I don't change this much.
    max_scans_in_parallel = 5

    # Schedule an initial scan for each segment of the table.  We read each
    # segment in a separate thread, then look to see if there are more rows to
    # read -- and if so, we schedule another scan.
    tasks_to_do = [
        {
            **kwargs,
            "TableName": table_name,
            "Segment": segment,
            "TotalSegments": total_segments,
        }
        for segment in range(total_segments)
    ]

    # Make the list an iterator, so the same tasks don't get run repeatedly.
    scans_to_run = iter(tasks_to_do)

    with concurrent.futures.ThreadPoolExecutor() as executor:

        # Schedule the initial batch of futures.  Here we assume that
        # max_scans_in_parallel < total_segments, so there's no risk that
        # the queue will throw an Empty exception.
        futures = {
            executor.submit(dynamo_client.scan, **scan_params): scan_params
            for scan_params in itertools.islice(scans_to_run, max_scans_in_parallel)
        }

        while futures:
            # Wait for the first future to complete.
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                yield from fut.result()["Items"]

                scan_params = futures.pop(fut)

                # A Scan reads up to N items, and tells you where it got to in
                # the LastEvaluatedKey.  You pass this key to the next Scan operation,
                # and it continues where it left off.
                try:
                    scan_params["ExclusiveStartKey"] = fut.result()["LastEvaluatedKey"]
                except KeyError:
                    break
                tasks_to_do.append(scan_params)

            # Schedule the next batch of futures.  At some point we might run out
            # of entries in the queue if we've finished scanning the table, so
            # we need to spot that and not throw.
            for scan_params in itertools.islice(scans_to_run, len(done)):
                futures[
                    executor.submit(dynamo_client.scan, **scan_params)
                ] = scan_params


def list_table_items(table_name):
    dynamo_client = boto3.resource("dynamodb").meta.client

    items_generator = get_table_items(dynamo_client, table_name=table_name)

    print(f"DynamoDB {table_name} files")
    print(
        "".join(
            [
                " * %s - %s\n" % (e["experimentId"], e["plotUuid"])
                for e in items_generator
            ]
        )
    )


@click.command()
@click.argument(
    "resource_name",
    default="biomage-source-production",
)
@click.argument(
    "resource",
    default="s3",
)
def ls(resource_name, resource):
    """
    Get all available items in a given resource like an S3 bucket or DynamoDB table.

    [RESOURCE_NAME]: name of the S3 bucket or DynamoDB table
     (default: "biomage-source-production")

    [RESOURCE]: either "s3" or "dynamodb"  (default: "s3")

    Examples:

    * List all experiments in production:

    biomage experiment ls biomage-source-production

    * List all the plots and tables configs in DynamoDB:

    biomage experiment ls plots-tables-production dynamodb
    """

    if resource == "s3":
        list_bucket_files(resource_name)
    else:
        list_table_items(resource_name)
