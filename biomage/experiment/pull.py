import boto3
import click
from botocore.exceptions import ClientError

from ..utils.constants import (
    CELLSETS_FILE,
    DEFAULT_EXPERIMENT_ID,
    EXPERIMENTS_FILE,
    EXPERIMENTS_TABLE,
    PLOTS_TABLES_FILE,
    PROCESSED_RDS_FILE,
    PRODUCTION,
    PROJECTS_FILE,
    PROJECTS_TABLE,
    SAMPLES_FILE,
    SAMPLES_TABLE,
    SOURCE_RDS_FILE,
)
from .utils import (
    PULL,
    Summary,
    add_env_user_to_experiment,
    download_S3_json,
    download_S3_rds,
    is_modified,
    load_cfg_file,
    save_cfg_file,
)


def download_S3_obj(s3_obj, key, filepath):
    if SOURCE_RDS_FILE in filepath or PROCESSED_RDS_FILE in filepath:
        download_S3_rds(s3_obj, key, filepath)
    elif CELLSETS_FILE in filepath:
        download_S3_json(s3_obj, key, filepath)
    else:
        raise ValueError(f"unexpected file: {filepath}")


def download_if_modified(bucket, key, filepath):
    s3 = boto3.resource("s3")
    s3_obj = s3.Object(bucket, key)

    try:
        s3_obj.last_modified
    except ClientError as e:
        raise Exception(f"could not retrieve file: {bucket}/{key}\n{e}")
    if is_modified(s3_obj, filepath):
        click.echo(
            f"Local file for key {key} last modified date "
            "differs from S3 version.\n Updating local copy"
        )

        download_S3_obj(s3_obj, key, filepath)
        Summary.add_changed_file(filepath)


def remove_key(dic, k):
    if k in dic:
        del dic[k]
    for val in dic.values():
        if isinstance(val, dict):
            remove_key(val, k)
    return dic


def update_project_config_if_needed(filepath, table_name, project_uuid):
    """
    Filepath: experiment_id/filename
    """

    local_cfg, found = load_cfg_file(filepath)

    client = boto3.client("dynamodb")
    try:
        remote_cfg = client.get_item(
            TableName=table_name, Key={"projectUuid": {"S": project_uuid}}
        )["Item"]
    except KeyError:
        raise ValueError(
            f"Project with ID {project_uuid} not found. If the ID does not look "
            + "like a UUID, you are probably pulling an old experiment without an "
            + "associated project."
        )

    # if the local config was not found or it's different from the remote => update
    if not found or local_cfg != remote_cfg:
        save_cfg_file(remote_cfg, filepath)
        Summary.add_changed_file(filepath)


def update_experiment_config_if_needed(filepath, table_name, experiment_id):
    """
    Filepath: experiment_id/filename
    """

    local_cfg, found = load_cfg_file(filepath)

    client = boto3.client("dynamodb")
    remote_cfg = client.get_item(
        TableName=table_name, Key={"experimentId": {"S": experiment_id}}
    )["Item"]

    # the "pipeline" field in experiment config has information about
    # the production pipeline Arn causing a crash with ExecutionDoesNotExist
    # locally in the API. This solution is not ideal as it will fail
    # if the field name changes or more tightly coupled info is added
    # TODO: make api handle not found cases, or ignore keys in development env
    remote_cfg = remove_key(remote_cfg, "pipeline")

    # add the cognito user specified in the env to the experiment permissions
    remote_cfg = add_env_user_to_experiment(cfg=remote_cfg)

    # if the local config was not found or it's different from the remote => update
    if not found or local_cfg != remote_cfg:
        save_cfg_file(remote_cfg, filepath)
        Summary.add_changed_file(filepath)

    return remote_cfg["projectId"]["S"]


def update_config_if_needed(filepath, table_name, experiment_id):
    """
    Filepath: experiment_id/filename
    """

    local_cfg, found = load_cfg_file(filepath)

    client = boto3.client("dynamodb")
    remote_cfg = client.get_item(
        TableName=table_name, Key={"experimentId": {"S": experiment_id}}
    )["Item"]

    # if the local config was not found or it's different from the remote => update
    if not found or local_cfg != remote_cfg:
        save_cfg_file(remote_cfg, filepath)
        Summary.add_changed_file(filepath)


def update_configs(experiment_id, origin):
    # update samples
    file_path = f"{experiment_id}/{SAMPLES_FILE}"
    table_name = f"{SAMPLES_TABLE}-{origin}"
    project_uuid = update_config_if_needed(file_path, table_name, experiment_id)

    # update experiments
    file_path = f"{experiment_id}/{EXPERIMENTS_FILE}"
    table_name = f"{EXPERIMENTS_TABLE}-{origin}"
    project_uuid = update_experiment_config_if_needed(
        file_path, table_name, experiment_id
    )

    # update projects
    file_path = f"{experiment_id}/{PROJECTS_FILE}"
    table_name = f"{PROJECTS_TABLE}-{origin}"
    update_project_config_if_needed(file_path, table_name, project_uuid=project_uuid)

    # plots and tables config has key issues (references that do no
    # exist locally), for now just create an empty json
    empty_plots_tables = {"records": []}
    filepath = f"{experiment_id}/{PLOTS_TABLES_FILE}"
    save_cfg_file(empty_plots_tables, filepath)
    Summary.add_changed_file(filepath)


@click.command()
@click.option(
    "-e",
    "--experiment_id",
    required=False,
    default=DEFAULT_EXPERIMENT_ID,
    help="Experiment ID to be copied.",
)
@click.option(
    "-i",
    "--input_env",
    required=False,
    default=PRODUCTION,
    help="Input environment to pull the data from.",
)
def pull(experiment_id, input_env):
    """
    Downloads experiment data and config files from a given environment.\n

    E.g.:
    biomage experiment pull -i staging -e e52b39624588791a7889e39c617f669e

    Works only with r.rds datasets.\n
    """

    Summary.set_command(cmd=PULL, origin=input_env, experiment_id=experiment_id)

    bucket = f"biomage-source-{input_env}"
    file = f"{experiment_id}/r.rds"
    dst_file = f"{experiment_id}/{SOURCE_RDS_FILE}.gz"
    download_if_modified(bucket=bucket, key=file, filepath=dst_file)

    bucket = f"processed-matrix-{input_env}"
    file = f"{experiment_id}/r.rds"
    dst_file = f"{experiment_id}/{PROCESSED_RDS_FILE}.gz"
    download_if_modified(bucket=bucket, key=file, filepath=dst_file)

    bucket = f"cell-sets-{input_env}"
    dst_file = f"{experiment_id}/{CELLSETS_FILE}"
    # the name of the cell sets file in S3 is just the experiment ID
    download_if_modified(bucket=bucket, key=experiment_id, filepath=dst_file)

    update_configs(experiment_id, input_env)

    Summary.report_changes()
