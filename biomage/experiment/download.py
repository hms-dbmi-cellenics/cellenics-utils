import json
import os
from pathlib import Path

import boto3
import click

from ..rds.run import run_rds_command
from ..utils.constants import (
    CELLSETS_BUCKET,
    DEFAULT_AWS_PROFILE,
    PROCESSED_FILES_BUCKET,
    RAW_FILES_BUCKET,
    SAMPLES_BUCKET,
    STAGING,
)

SAMPLES = "samples"
RAW_FILE = "raw_rds"
PROCESSED_FILE = "processed_rds"
CELLSETS = "cellsets"

SANDBOX_ID = "default"
REGION = "eu-west-1"
USER = "dev_role"

file_type_to_name_map = {
    "features10x": "features.tsv.gz",
    "matrix10x": "matrix.mtx.gz",
    "barcodes10x": "barcodes.tsv.gz",
}

DATA_LOCATION = os.getenv("BIOMAGE_DATA_PATH", "./data")


def _download_file(bucket, s3_path, file_path, boto3_session):
    s3 = boto3_session.resource("s3")

    file_path.parent.mkdir(parents=True, exist_ok=True)

    s3_obj = s3.Object(bucket, s3_path)
    s3_obj.download_file(str(file_path))


def _create_sample_mapping(samples_list, output_path):
    """
    Create a mapping of sample names to sample ids and writes them to a file.
    """

    MAPPING_FILE_NAME = "sample_mapping.json"

    samples_file = output_path / MAPPING_FILE_NAME

    sample_mapping = {}

    for sample_name, samples in samples_list.items():
        sample_mapping[sample_name] = samples[0]["sample_id"]

    samples_file.write_text(json.dumps(sample_mapping))

    print(f"Sample name-id map downloaded to: {str(samples_file)}.\n")


def _process_query_output(query_result):
    json_text = (
        query_result.replace("+", "")
        .split("\n", 2)[2]
        .replace("\n", "")
        .replace("(1 row)", "")
        .strip()
    )

    if not json_text:
        raise Exception("No data returned from query")

    return json.loads(json_text)


def _query_db(query, input_env, aws_profile):
    query = f"""psql -c "SELECT json_agg(q) FROM ( {query} ) AS q" """

    return _process_query_output(
        run_rds_command(
            query, SANDBOX_ID, input_env, USER, REGION, aws_profile, capture_output=True
        )
    )


def _get_experiment_samples(experiment_id, input_env, aws_profile):
    query = f"""
        SELECT id as sample_id, name as sample_name \
            FROM sample WHERE experiment_id = '{experiment_id}'
    """

    return _query_db(query, input_env, aws_profile)


def _get_sample_files(sample_ids, input_env, aws_profile):
    query = f""" SELECT sample_id, s3_path, sample_file_type FROM sample_file \
            INNER JOIN sample_to_sample_file_map \
            ON sample_to_sample_file_map.sample_file_id = sample_file.id \
            WHERE sample_to_sample_file_map.sample_id IN ('{ "','".join(sample_ids) }')
    """

    return _query_db(query, input_env, aws_profile)


def _get_samples(experiment_id, input_env, aws_profile):
    print(f"Querying samples for {experiment_id}...")
    samples = _get_experiment_samples(experiment_id, input_env, aws_profile)

    sample_id_to_name = {}
    for sample in samples:
        sample_id_to_name[sample["sample_id"]] = sample["sample_name"]

    print(f"Querying sample files for {experiment_id}...")
    sample_ids = [entry["sample_id"] for entry in samples]
    sample_files = _get_sample_files(sample_ids, input_env, aws_profile)

    result = {}
    for sample_file in sample_files:
        sample_id = sample_file["sample_id"]
        sample_name = sample_id_to_name[sample_id]

        if not result.get(sample_name):
            result[sample_name] = []

        result[sample_name].append(
            {
                "sample_id": sample_id,
                "sample_name": sample_name,
                "s3_path": sample_file["s3_path"],
                "sample_file_name": file_type_to_name_map[
                    sample_file["sample_file_type"]
                ],
            }
        )

    return result


def _download_samples(
    experiment_id,
    input_env,
    output_path,
    use_sample_id_as_name,
    boto3_session,
    aws_account_id,
    aws_profile,
):
    bucket = f"{SAMPLES_BUCKET}-{input_env}-{aws_account_id}"

    samples_list = _get_samples(experiment_id, input_env, aws_profile)
    num_samples = len(samples_list)

    print(f"\n{num_samples} samples found. Downloading sample files...\n")

    for sample_idx, value in enumerate(samples_list.items()):
        sample_name, sample_files = value

        if use_sample_id_as_name:
            sample_name = sample_files[0]["sample_id"]

        num_files = len(sample_files)

        print(
            f"Downloading files for sample {sample_name} (sample {sample_idx+1}/{num_samples})",
        )

        for file_idx, sample_file in enumerate(sample_files):
            s3_path = sample_file["s3_path"]

            file_name = sample_file["sample_file_name"]
            file_path = output_path / sample_name / file_name

            print(f"> Downloading {s3_path} (file {file_idx+1}/{num_files})")

            s3client = boto3_session.client("s3")
            s3client.head_object(Bucket=bucket, Key=s3_path)
            _download_file(bucket, s3_path, file_path, boto3_session)

        print(f"Sample {sample_name} downloaded.\n")

    _create_sample_mapping(samples_list, output_path)
    click.echo(
        click.style(
            "All samples for the experiment have been downloaded.",
            fg="green",
        )
    )


def _download_raw_rds_files(
    experiment_id,
    input_env,
    output_path,
    use_sample_id_as_name,
    boto3_session,
    aws_account_id,
    aws_profile,
):

    bucket = f"{SAMPLES_BUCKET}-{input_env}-{aws_account_id}"

    sample_list = _get_experiment_samples(experiment_id, input_env, aws_profile)
    num_samples = len(sample_list)

    print(f"\n{num_samples} samples found. Downloading raw rds files...\n")

    bucket = f"{RAW_FILES_BUCKET}-{input_env}-{aws_account_id}"
    end_message = "Raw RDS files have been downloaded."

    for sample_idx, sample in enumerate(sample_list):

        s3_path = f"{experiment_id}/{sample['sample_id']}/r.rds"

        if use_sample_id_as_name:
            file_name = f"{sample['sample_id']}.rds"
        else:
            file_name = f"{sample['sample_name']}.rds"

        file_path = output_path / "raw" / f"{file_name}.rds"

        print(f"Downloading {file_name} ({sample_idx+1}/{num_samples})")

        s3client = boto3_session.client("s3")
        s3client.head_object(Bucket=bucket, Key=s3_path)
        _download_file(bucket, s3_path, file_path, boto3_session)

        print(f"Sample {sample['sample_name']} downloaded.\n")

    print(end_message)


def _download_processed_rds_file(
    experiment_id,
    input_env,
    output_path,
    boto3_session,
    aws_account_id,
):

    file_name = "processed_r.rds"
    bucket = f"{PROCESSED_FILES_BUCKET}-{input_env}-{aws_account_id}"
    end_message = "Processed RDS files have been downloaded."

    key = f"{experiment_id}/r.rds"
    file_path = output_path / file_name

    _download_file(bucket, key, file_path, boto3_session)

    print(f"RDS file saved to {file_path}")
    click.echo(click.style(f"{end_message}", fg="green"))


def _download_cellsets(
    experiment_id, input_env, output_path, boto3_session, aws_account_id
):
    FILE_NAME = "cellsets.json"

    bucket = f"{CELLSETS_BUCKET}-{input_env}-{aws_account_id}"
    key = experiment_id
    file_path = output_path / FILE_NAME
    _download_file(bucket, key, file_path, boto3_session)
    print(f"Cellsets file saved to {file_path}")
    click.echo(click.style("Cellsets file have been downloaded.", fg="green"))


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
    default=STAGING,
    show_default=True,
    help="Input environment to pull the data from.",
)
@click.option(
    "-o",
    "--output_path",
    required=False,
    default=DATA_LOCATION,
    show_default=True,
    help="Output path. By default points to BIOMAGE_DATA_PATH/experiment_id.",
)
@click.option(
    "-a",
    "--all",
    required=False,
    is_flag=True,
    default=False,
    show_default=True,
    help="Download all files for the experiment.",
)
@click.option(
    "--name_with_id",
    required=False,
    is_flag=True,
    default=False,
    show_default=True,
    help="Use sample id to name samples.",
)
@click.option(
    "-f",
    "--files",
    multiple=True,
    required=False,
    default=[SAMPLES],
    show_default=True,
    help=(
        "Files to download. By default only the samples (-f samples) are downloaded. "
        "You can also download cellsets (-f cellsets), raw RDS (-f raw_rds) and "
        "processed RDS (-f processed_rds)."
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
def download(
    experiment_id, input_env, output_path, files, all, name_with_id, aws_profile
):
    """
    Downloads files associated with an experiment from a given environment.\n
    It requires an open tunnel to the desired environment to fetch data from SQL:
    `biomage rds tunnel -i staging`

    E.g.:
    biomage experiment download -i staging -e 2093e95fd17372fb558b81b9142f230e
    -f samples -f cellsets -o output/folder
    """

    boto3_session = boto3.Session(profile_name=aws_profile)
    aws_account_id = boto3_session.client("sts").get_caller_identity().get("Account")

    # Set output path
    # By default add experiment_id to the output path
    if output_path == DATA_LOCATION:
        output_path = Path(os.path.join(DATA_LOCATION, experiment_id))
    else:
        output_path = Path(os.getcwd()) / output_path

    print("Saving downloaded files to: ", str(output_path))

    selected_files = []
    if all:
        selected_files = [SAMPLES, CELLSETS, RAW_FILE, PROCESSED_FILE]
    else:
        selected_files = list(files)

    for file in selected_files:
        if file == SAMPLES:
            print("\n== Downloading sample files")
            try:
                _download_samples(
                    experiment_id,
                    input_env,
                    output_path,
                    name_with_id,
                    boto3_session,
                    aws_account_id,
                    aws_profile,
                )
            except Exception as e:

                message = e.args[0]
                if "No data returned from query" in message:
                    click.echo(
                        click.style(
                            "This experiment does not exist in the RDS database.\n"
                            "Try dowloading it directly from S3.",
                            fg="yellow",
                        )
                    )
                    return

                raise e

        elif file == RAW_FILE:
            print("\n== Downloading raw RDS file")
            _download_raw_rds_files(
                experiment_id,
                input_env,
                output_path,
                name_with_id,
                boto3_session,
                aws_account_id,
                aws_profile,
            )

        elif file == PROCESSED_FILE:
            print("\n== Downloading processed RDS file")
            _download_processed_rds_file(
                experiment_id,
                input_env,
                output_path,
                boto3_session,
                aws_account_id,
            )

        elif file == CELLSETS:
            print("\n== Download cellsets file")
            _download_cellsets(
                experiment_id, input_env, output_path, boto3_session, aws_account_id
            )
