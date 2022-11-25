import os
from pathlib import Path

import boto3
import click

from ..utils.AuroraClient import AuroraClient
from ..utils.constants import (
    CELLSETS_BUCKET,
    DEFAULT_AWS_PROFILE,
    PROCESSED_FILES_BUCKET,
    RAW_FILES_BUCKET,
    STAGING,
)

SAMPLES = "samples"
RAW_FILE = "raw_rds"
PROCESSED_FILE = "processed_rds"
CELLSETS = "cellsets"
SAMPLE_MAPPING = "sample_mapping"

SANDBOX_ID = "default"
REGION = "eu-west-1"
USER = "dev_role"

file_type_to_name_map = {
    "features10x": "features.tsv.gz",
    "matrix10x": "matrix.mtx.gz",
    "barcodes10x": "barcodes.tsv.gz",
}

DATA_LOCATION = os.getenv("BIOMAGE_DATA_PATH", "./data")


def _upload_file(bucket, s3_path, file_path, boto3_session):
    s3 = boto3_session.resource("s3")

    print(f"{file_path}, {bucket}, {s3_path}")
    s3.meta.client.upload_file(str(file_path), bucket, s3_path)


def _get_experiment_samples(experiment_id, aurora_client):
    query = f"""
        SELECT id as sample_id, name as sample_name \
            FROM sample WHERE experiment_id = '{experiment_id}'
    """

    return aurora_client.select(query)


def _upload_raw_rds_files(
    experiment_id,
    output_env,
    input_path,
    without_tunnel,
    boto3_session,
    aws_account_id,
    aws_profile,
):
    bucket = f"{RAW_FILES_BUCKET}-{output_env}-{aws_account_id}"
    local_folder_path = os.path.join(input_path, f"{experiment_id}/raw")
    end_message = "Raw RDS files have been uploaded."

    if without_tunnel:
        print(
            """IMPORTANT: rds tunnel disabled, local folder is expected to have the
            structure <experiment_id>/<sample_id>/r.rds"""
        )
        for root, dirs, files in os.walk(local_folder_path):
            for file_name in files:
                local_path = os.path.join(root, file_name)

                # root ends with /<sample_id>
                sample_id = root.split("/")[-1]

                s3_path = f"{experiment_id}/{sample_id}/r.rds"

                print(f"\t= Uploading {local_path} to {s3_path}")
                _upload_file(bucket, s3_path, local_path, boto3_session)
        print(end_message)

        return

    with AuroraClient(
        SANDBOX_ID, USER, REGION, output_env, aws_profile
    ) as aurora_client:
        sample_list = _get_experiment_samples(experiment_id, aurora_client)

    num_samples = len(sample_list)

    print(f"\n{num_samples} samples found. Uploading raw rds files...\n")

    for sample_idx, sample in enumerate(sample_list):
        sample_id = sample["sample_id"]
        sample_name = sample["sample_name"]

        s3_path = f"{experiment_id}/{sample_id}/r.rds"

        file_path = input_path / "raw" / f"{sample_name}.rds"

        print(f"uploading {sample_name} ({sample_idx+1}/{num_samples})")

        _upload_file(bucket, s3_path, file_path, boto3_session)

        print(f"Sample {sample_name} uploaded.\n")

    print(end_message)


def _upload_processed_rds_file(
    experiment_id,
    output_env,
    input_path,
    boto3_session,
    aws_account_id,
):

    file_name = "processed_r.rds"
    bucket = f"{PROCESSED_FILES_BUCKET}-{output_env}-{aws_account_id}"
    end_message = "Processed RDS files have been uploaded."

    key = f"{experiment_id}/r.rds"
    file_path = input_path / file_name

    _upload_file(bucket, key, file_path, boto3_session)

    print(f"RDS file saved to {file_path}")
    click.echo(click.style(f"{end_message}", fg="green"))


def _upload_cellsets(
    experiment_id, output_env, input_path, boto3_session, aws_account_id
):
    FILE_NAME = "cellsets.json"

    bucket = f"{CELLSETS_BUCKET}-{output_env}-{aws_account_id}"
    key = experiment_id
    file_path = input_path / FILE_NAME
    _upload_file(bucket, key, file_path, boto3_session)
    click.echo(
        click.style(f"Cellsets file have been uploaded to {experiment_id}.", fg="green")
    )


@click.command()
@click.option(
    "-e",
    "--experiment_id",
    required=True,
    help="Experiment ID to be copied.",
)
@click.option(
    "-o",
    "--output_env",
    required=True,
    default=STAGING,
    show_default=True,
    help="Output environment to upload the data to.",
)
@click.option(
    "-i",
    "--input_path",
    required=False,
    default=DATA_LOCATION,
    show_default=True,
    help="Input path. By default points to BIOMAGE_DATA_PATH/experiment_id.",
)
@click.option(
    "-a",
    "--all",
    required=False,
    is_flag=True,
    default=False,
    show_default=True,
    help="upload all files for the experiment.",
)
@click.option(
    "-f",
    "--files",
    multiple=True,
    required=True,
    show_default=True,
    help=(
        "Files to upload. You can also upload cellsets (-f cellsets), raw RDS "
        "(-f raw_rds) and processed RDS (-f processed_rds)."
    ),
)
@click.option(
    "--without_tunnel",
    required=False,
    is_flag=True,
    default=False,
    show_default=True,
    help=(
        "Dont use the rds tunnel. "
        "If set, the raw samples must be stored by sample id instead of sample name"
    ),
)
@click.option(
    "-p",
    "--aws_profile",
    required=False,
    default=DEFAULT_AWS_PROFILE,
    show_default=True,
    help="The name of the profile stored in ~/.aws/credentials to use.",
)
def upload(
    experiment_id, output_env, input_path, files, all, without_tunnel, aws_profile
):
    """
    Uploads the files in input_path into the specified experiment_id and environment.\n
    It requires an open tunnel to the desired environment to fetch data from SQL:
    `biomage rds tunnel -i staging`

    E.g.:
    biomage experiment upload -o staging -e 2093e95fd17372fb558b81b9142f230e
    -f samples -f cellsets -o output/folder
    """

    boto3_session = boto3.Session(profile_name=aws_profile)
    aws_account_id = boto3_session.client("sts").get_caller_identity().get("Account")

    # Set output path
    # By default add experiment_id to the output path
    if input_path == DATA_LOCATION:
        input_path = Path(os.path.join(DATA_LOCATION, experiment_id))
    else:
        input_path = Path(os.getcwd()) / input_path

    print("Uploading files from: ", str(input_path))

    selected_files = []
    if all:
        selected_files = [CELLSETS, RAW_FILE, PROCESSED_FILE]
    else:
        selected_files = list(files)

    print(f"files: {files}")
    for file in selected_files:
        if file == SAMPLES:
            print("\n== Uploading sample files is not supported")

        elif file == RAW_FILE:
            print("\n== uploading raw RDS file")
            _upload_raw_rds_files(
                experiment_id,
                output_env,
                input_path,
                without_tunnel,
                boto3_session,
                aws_account_id,
                aws_profile,
            )

        elif file == PROCESSED_FILE:
            print("\n== uploading processed RDS file")
            _upload_processed_rds_file(
                experiment_id,
                output_env,
                input_path,
                boto3_session,
                aws_account_id,
            )

        elif file == CELLSETS:
            print("\n== upload cellsets file")
            _upload_cellsets(
                experiment_id, output_env, input_path, boto3_session, aws_account_id
            )
        else:
            print(f"\n== Unknown file option {file}")
