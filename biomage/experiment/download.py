import json
import os
from enum import Enum
from fileinput import filename
from pathlib import Path

import boto3
import click
from botocore.exceptions import ClientError

from ..rds.run import run_rds_command
from ..utils.constants import (CELLSETS_FILE, DEFAULT_EXPERIMENT_ID,
                               EXPERIMENTS_FILE, EXPERIMENTS_TABLE,
                               PLOTS_TABLES_FILE, PROCESSED_RDS_FILE,
                               PRODUCTION, PROJECTS_FILE, PROJECTS_TABLE,
                               SAMPLES_FILE, SAMPLES_TABLE, SOURCE_RDS_FILE)
from .utils import set_modified_date

output_path = "."


class FileType(Enum):
    SAMPLES = "samples"
    RDS = "rds"
    CELLSETS = "cellsets"


s3 = boto3.resource("s3")

# def get_s3_object(experiment_id, input_env, file):

#     s3_obj = None
#     file_path = None

#     if file == FileType.SAMPLES:
#         s3_obj = s3.Object(bucket, key)


# def download_file(s3_obj, filepath):
#     """
#     Download s3 file to filepath
#     """
#     Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)

#     s3_obj = get_s3_object(experiment_id, input_env, file)

#     s3_obj.download_file(filepath)

#     set_modified_date(file_location=filepath, date=s3_obj.last_modified)


def _process_query_output(result_str):
    json_text = (
        result_str.replace("+", "")
        .split("\n", 2)[2]
        .replace("\n", "")
        .replace("(1 row)", "")
        .strip()
    )

    samples = json.loads(json_text)

    # Create a dictionary of samples

    result = {}

    for sample in samples:

        if not result.get(sample["sample_name"]):
            result[sample["sample_name"]] = []

        result[sample["sample_name"]].append(sample)

    return result


def download_samples(experiment_id, input_env, output_path):
    """
    Download samples associated with an experiment from a given environment.\n
    """

    SANDBOX_ID = "default"
    REGION = "eu-west-1"
    bucket = f"biomage-originals-{input_env}"

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

    user = "dev_role"

    print(f"Querying samples for {experiment_id}...")

    result_str = run_rds_command(command, SANDBOX_ID, input_env, user, REGION, True)
    samples_list = _process_query_output(result_str)

    print(f"{len(samples_list)} samples found. Downloading sample files...\n")

    for sample_name, samples in samples_list.items():

        print(
            f"Downloading files for sample {sample_name}",
        )

        for sample in samples:

            s3_path = sample["s3_path"]
            file_name = Path(s3_path).name

            print(f"> Downloading {sample_name}/{file_name}...")

            file_path = output_path / sample_name / file_name
            file_path.parent.mkdir(parents=True, exist_ok=True)

            s3_obj = s3.Object(bucket, s3_path)
            s3_obj.download_file(str(file_path))

            set_modified_date(file_location=file_path, date=s3_obj.last_modified)

        print(f"Sample {sample_name} downloaded.\n")

    print(f"All samples for experiment {experiment_id} downloaded.\n")


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
    required=False,
    default=PRODUCTION,
    show_default=True,
    help="Input environment to pull the data from.",
)
@click.option(
    "-d",
    "--directory",
    required=False,
    default=None,
    help="Name of output folder. By default this will be the experiment id.",
)
@click.option(
    "-f",
    "--files",
    multiple=True,
    required=False,
    default=[file_type.value for file_type in FileType],
    show_default=True,
    help="Files to download. By default all (samples, rds, cellsets) are downloaded.",
)
def download(experiment_id, input_env, directory, files):
    """
    Downloads files associated with an experiment from a given environment.\n

    E.g.:
    biomage experiment download -i staging -e 2093e95fd17372fb558b81b9142f230e
    -f samples -f rds -o output/folder
    """

    # Set output path
    # By default, the output path is named after the experiment id
    if not directory:
        directory = experiment_id

    output_path = Path(os.getcwd()) / directory

    # Create intermediary foldrs if they don't exist
    if not output_path.exists():
        print("Creating output folder:", output_path)
        # output_path.mkdir(parents=True, exist_ok=True)

    download_samples(experiment_id, input_env, output_path)

    # # Download S3 object
    # for file in files:

    #     if file == FileType.SAMPLES:
    #         download_samples(experiment_id, input_env, output_path)

    #     elif file == FileType.RDS:
    #         pass

    #     elif file == FileType.CELLSETS:
    #         pass
