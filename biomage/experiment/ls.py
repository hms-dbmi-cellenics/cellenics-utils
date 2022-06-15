import boto3
import click

from ..utils.constants import PRODUCTION


def list_bucket_files(bucket):
    s3 = boto3.client("s3")  # low-level functional API
    objects = s3.list_objects(Bucket=bucket).get("Contents", [])
    print(f"Bucket {bucket} files")
    print("".join([" * %s\n" % f["Key"] for f in objects]))


@click.command()
@click.argument(
    "origin",
    default=PRODUCTION,
)
def ls(origin):
    """
    Get all available experiments in the bucket 'biomage-source-{origin}-242905224710 in S3.
    """

    bucket = f"biomage-source-{origin}-242905224710"
    list_bucket_files(bucket)
