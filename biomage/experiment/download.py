import json
import os
from enum import Enum
from fileinput import filename
from pathlib import Path

import boto3
import click
from botocore.exceptions import ClientError

from ..rds.run import run_rds_command
from ..utils.constants import (CELLSETS_BUCKET, PROCESSED_RDS_BUCKET,
                               RAW_RDS_BUCKET, SAMPLES_BUCKET, SAMPLES_TABLE)
from .utils import set_modified_date

output_path = "."


s3 = boto3.resource("s3")
SAMPLES = "samples"
RAW_RDS = "raw_rds"
PROCESSED_RDS = "qc_rds"
CELLSETS = "cellsets"


def _download_file(bucket, s3_path, file_path):
    file_path.parent.mkdir(parents=True, exist_ok=True)

    s3_obj = s3.Object(bucket, s3_path)
    s3_obj.download_file(str(file_path))
    set_modified_date(file_location=file_path, date=s3_obj.last_modified)


def _create_sample_mapping(samples_list, output_path):
    """
    Create a mapping of sample names to sample ids.
    """

    MAPPING_FILE_NAME = "sample_mapping.json"

    samples_file = output_path / MAPPING_FILE_NAME

    sample_mapping = {}

    for sample_name, samples in samples_list.items():
        sample_mapping[sample_name] = samples[0]["sample_id"]

    samples_file.write_text(json.dumps(sample_mapping))

    print(f"Sample name-id map downloaded to: {str(samples_file)}.\n")


def _download_samples_v1(experiment_id, input_env, output_path):
    """
    Download samples associated with an experiment from a given environment for v1.\n
    """
    bucket = f"{SAMPLES_BUCKET}-{input_env}"
    table = f"{SAMPLES_TABLE}-{input_env}"

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table)

    response = table.get_item(Key={"experimentId": experiment_id})

    samples_list = {}

    project_id = response["Item"]["projectUuid"]
    samples = response["Item"]["samples"]

    num_samples = len(samples)
    print(f"\n{num_samples} samples found. Downloading sample files...\n")

    for sample_idx, value in enumerate(samples.items()):
        sample_id, sample = value

        sample_name = sample["name"]
        samples_list[sample_name] = [{"sample_id": sample_id}]

        print(
            f"Downloading files for sample {sample_name} (sample {sample_idx+1}/{num_samples})",
        )

        num_files = len(sample["fileNames"])

        for file_idx, file_name in enumerate(sample["fileNames"]):
            s3_path = f"{project_id}/{sample_id}/{file_name}"
            file_path = output_path / sample_name / file_name

            print(
                f"> Downloading {sample_name}/{file_name} (file {file_idx+1}/{num_files})"
            )

            _download_file(bucket, s3_path, file_path)

        print(f"Sample {sample_name} downloaded.")

    _create_sample_mapping(samples_list, output_path)


def _process_query_output(result_str):
    json_text = (
        result_str.replace("+", "")
        .split("\n", 2)[2]
        .replace("\n", "")
        .replace("(1 row)", "")
        .strip()
    )

    if not json_text:
        raise Exception("No data returned from query")

    samples = json.loads(json_text)

    # Create a dictionary of samples
    result = {}

    for sample in samples:

        if not result.get(sample["sample_name"]):
            result[sample["sample_name"]] = []

        result[sample["sample_name"]].append(sample)

    return result


def _get_samples_v2(experiment_id, input_env):
    SANDBOX_ID = "default"
    REGION = "eu-west-1"
    USER = "dev_role"

    command = f"""psql -c "SELECT json_agg(t) FROM ( \
        SELECT sample_file_id , sample_id, s3_path, name AS sample_name FROM ( \
            SELECT sample_file_id, sample_id, s3_path FROM ( \
                SELECT  sample_file_id, sample_id \
                    FROM sample_to_sample_file_map \
                    WHERE sample_id \
                        IN ( \
                        SELECT id \
                        FROM sample \
                        WHERE experiment_id = '{experiment_id}' \
                    ) \
            ) AS a \
            LEFT JOIN sample_file sf ON a.sample_file_id = sf.id \
        ) AS b \
        INNER JOIN sample ON b.sample_id = sample.id ) as t"\
    """

    print(f"Querying samples for {experiment_id}...")

    result_str = run_rds_command(command, SANDBOX_ID, input_env, USER, REGION, True)
    return _process_query_output(result_str)


def _download_samples_v2(experiment_id, input_env, output_path):
    """
    Download samples associated with an experiment from a given environment for v2.\n
    """
    bucket = f"{SAMPLES_BUCKET}-{input_env}"

    samples_list = _get_samples_v2(experiment_id, input_env)
    num_samples = len(samples_list)

    print(f"{num_samples} samples found. Downloading sample files...\n")

    for sample_idx, value in enumerate(samples_list.items()):
        sample_name, samples = value
        num_files = len(samples)

        print(
            f"Downloading files for sample {sample_name} (sample {sample_idx+1}/{num_samples})",
        )

        for file_idx, sample in enumerate(samples):

            s3_path = sample["s3_path"]
            file_name = Path(s3_path).name
            file_path = output_path / sample_name / file_name

            print(
                f"> Downloading {sample_name}/{file_name} (file {file_idx+1}/{num_files})"
            )

            s3client = boto3.client("s3")
            s3client.head_object(Bucket=bucket, Key=s3_path)

            _download_file(bucket, s3_path, file_path)

        print(f"Sample {sample_name} downloaded.\n")

    _create_sample_mapping(samples_list, output_path)


def _download_rds(experiment_id, input_env, output_path, processed=False):
    file_name = None
    bucket = None

    if not processed:
        file_name = "raw_r.rds"
        bucket = f"{RAW_RDS_BUCKET}-{input_env}"
    else:
        file_name = "processed_r.rds"
        bucket = f"{PROCESSED_RDS_BUCKET}-{input_env}"

    key = f"{experiment_id}/r.rds"
    file_path = output_path / file_name

    _download_file(bucket, key, file_path)

    print(f"RDS file saved to {file_path}")


def _download_cellsets(experiment_id, input_env, output_path):
    FILE_NAME = "cellsets.json"

    bucket = f"{CELLSETS_BUCKET}-{input_env}"
    key = experiment_id
    file_path = output_path / FILE_NAME
    _download_file(bucket, key, file_path)
    print(f"Cellsets file saved to {file_path}")


@click.command()
@click.option(
    "-e",
    "--experiment_id",
    required=True,
    help="Experiment ID to be copied.",
)
@click.option(
    "-i",
    "--input_env",
    required=True,
    help="Input environment to pull the data from.",
)
@click.option(
    "-o",
    "--output_path",
    required=False,
    default=None,
    help="Name of output folder. By default this will be the experiment id.",
)
@click.option(
    "-a",
    "--all",
    required=False,
    is_flag=True,
    default=False,
    help="Download all files for the experiment.",
)
@click.option(
    "-f",
    "--files",
    multiple=True,
    required=False,
    default=[SAMPLES],
    help=(
        "Files to download. By default only the samples (-f samples) are downloaded. "
        "You can also download cellsets (-f cellsets), raw RDS (-f raw_rds) and "
        "processed RDS (-f qc_rds)."
    ),
)
def download(experiment_id, input_env, output_path, files, all):
    """
    Downloads files associated with an experiment from a given environment.\n

    E.g.:
    biomage experiment download -i staging -e 2093e95fd17372fb558b81b9142f230e
    -f samples -f cellsets -o output/folder
    """

    # Set output path
    # By default, the output path is named after the experiment id
    if not output_path:
        output_path = experiment_id

    output_path = Path(os.getcwd()) / output_path

    print("Saving downloaded files to: ", str(output_path))

    selected_files = []
    if all:
        selected_files = [SAMPLES, CELLSETS, RAW_RDS, PROCESSED_RDS]
    else:
        selected_files = list(files)

    for file in selected_files:
        if file == SAMPLES:
            print("\n== Downloading sample files")
            try:
                _download_samples_v2(experiment_id, input_env, output_path)
            except Exception:
                _download_samples_v1(experiment_id, input_env, output_path)
            print("All samples for the experiment have been downloaded.")

        elif file == RAW_RDS:
            print("\n== Downloading unprocessed RDS file")
            _download_rds(experiment_id, input_env, output_path)
            print("Raw RDS file have been downloaded.\n")

        elif file == PROCESSED_RDS:
            print("\n== Downloading processed RDS file")
            _download_rds(experiment_id, input_env, output_path, processed=True)
            print("Procssed RDS have been downloaded.")

        elif file == CELLSETS:
            print("\n== Download cellsets file")
            _download_cellsets(experiment_id, input_env, output_path)
            print("Cellsets file have been downloaded.")
