import boto3
import click
from deepdiff import DeepDiff

from .utils import (
    PULL,
    Summary,
    download_S3_obj,
    is_modified,
    load_cfg_file,
    save_cfg_file,
)


def download_if_modified(bucket, key):
    s3 = boto3.resource("s3")
    s3_obj = s3.Object(bucket, key)

    if is_modified(s3_obj, key):
        click.echo(
            f"Local file for key {key} last modified date "
            "differs from S3 version.\n Updating local copy"
        )

        download_S3_obj(s3_obj, key)
        Summary.add_changed_file(key + ".gz")


def remove_key(dic, k):
    if k in dic:
        del dic[k]
    for val in dic.values():
        if isinstance(val, dict):
            remove_key(val, k)
    return dic


def update_config_if_needed(file, table_name, experiment_id):
    dynamodb = boto3.resource("dynamodb")

    local_cfg, found = load_cfg_file(file)
    remote_cfg = dynamodb.Table(table_name).get_item(
        Key={"experimentId": experiment_id}
    )["Item"]

    # the "pipeline" field in experiment config has information about
    # the production pipeline Arn causing a crash with ExecutionDoesNotExist
    # locally in the API. This solution is not ideal as it will fail
    # if the field name changes or more tightly coupled info is added
    # TODO: make api handle not found cases, or ignore keys in development env
    if "experiment" in table_name:
        remote_cfg = remove_key(remote_cfg, "pipeline")

    # if the local config was not found or it's different from the remote => update
    if not found or DeepDiff(local_cfg, remote_cfg):
        save_cfg_file(remote_cfg, file)
        Summary.add_changed_file(file)


def update_configs(experiment_id, origin):
    # config pairs like: (local file name, remote table name)
    configs = [
        ("mock_experiment.json", "experiments"),
        ("mock_samples.json", "samples"),
    ]

    for file_name, table_name in configs:
        file_path = f"{experiment_id}/{file_name}"
        table_name = f"{table_name}-{origin}"
        update_config_if_needed(file_path, table_name, experiment_id)

    # plots and tables config has key issues (references that do no
    # exist locally), for now just create an empty json
    empty_plots_tables = {"records": []}
    filename = "mock_plots_tables.json"
    save_cfg_file(empty_plots_tables, filename)
    Summary.add_changed_file(filename)


@click.command()
@click.argument(
    "origin",
    default="production",
)
@click.argument(
    "experiment_id",
    default="e52b39624588791a7889e39c617f669e",
    required=False,
)
def pull(experiment_id, origin):
    """
    Downloads experiment data and config files from a given environment.\n

    [EXPERIMENT_ID]: experiment to get (default: e52b39624588791a7889e39c617f669e)

    [ORIGIN]: environmnent to fetch the data from (default: production)

    Works only with r.rds datasets.\n
    """

    Summary.set_command(cmd=PULL, origin=origin, experiment_id=experiment_id)
    bucket = f"biomage-source-{origin}"
    file = f"{experiment_id}/r.rds"

    download_if_modified(bucket, file)

    update_configs(experiment_id, origin)

    Summary.report_changes()
