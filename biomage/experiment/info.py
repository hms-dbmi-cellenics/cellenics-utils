import json
import re

import boto3
import click
from tabulate import tabulate

from ..utils.AuroraClient import AuroraClient
from ..utils.constants import DEFAULT_AWS_PROFILE, STAGING

SAMPLES = "samples"
RAW_FILE = "raw_rds"
PROCESSED_FILE = "processed_rds"
FILTERED_CELLS = "filtered_cells"
CELLSETS = "cellsets"
SAMPLE_MAPPING = "sample_mapping"

SANDBOX_ID = "default"
REGION = "eu-west-1"
USER = "dev_role"


def _get_experiment_info(aurora_client, experiment_id):
    query = f"""
        SELECT id as experiment_id, name as experiment_name, created_at, pod_cpus, pod_memory \
            FROM experiment WHERE id = '{experiment_id}'
    """
    return aurora_client.run_query(query)[0]


def _get_user_cognito_info(
        users,
        env,
        attributes=['name', 'email', 'custom:agreed_terms', 'custom:agreed_emails'],
    ):
    cognito = boto3.client('cognito-idp')

    userpools = cognito.list_user_pools(
        MaxResults=60
    )['UserPools']

    userpool = [
        pool for pool in userpools
        if re.match(f'biomage-.*-{env}', pool['Name'])
    ][0]

    for user in users:
        u = cognito.admin_get_user(
            UserPoolId=userpool['Id'],
            Username=user['user_id']
        )

        for attr in u['UserAttributes']:
            if attr['Name'] in attributes:
                user[attr['Name']] = attr['Value']

    return users


def _get_experiment_users(aurora_client, experiment_id, env):
    query = f"""
        SELECT user_id, access_role \
            FROM user_access WHERE experiment_id = '{experiment_id}'
    """

    try:
        users = aurora_client.run_query(query)
        return _get_user_cognito_info(users, env)
    except:
        return []


def _get_experiment_samples(aurora_client, experiment_id):
    query = f"""
        SELECT id as sample_id, name, sample_technology, options \
            FROM sample WHERE experiment_id = '{experiment_id}'
    """

    try:
        return aurora_client.run_query(query)
    except:
        return []


def _get_experiment_runs(aurora_client, experiment_id):
    query = f"""
        SELECT pipeline_type, execution_arn, last_status_response \
            FROM experiment_execution WHERE experiment_id = '{experiment_id}'
    """

    try:
        return aurora_client.run_query(query)
    except:
        return []


def _print_tabbed(key, value):
    print(f"{key}\t\t: {value}")


def _format_item(details):
    for key, value in details.items():
        _print_tabbed(key, value)


def _format_table(content):

    header = list(content[0].keys())
    table = []

    for item in content:
        table.append(list(item.values()))

    print(tabulate(table, header, tablefmt="simple"))


def _format_runs(content):
    for run in content:
        print(run['pipeline_type'].upper())
        _print_tabbed('execution_arn', run['execution_arn'])

        run_details = run['last_status_response'][run['pipeline_type']]

        _print_tabbed('status\t', run_details['status'])
        if(run_details['status'] != "SUCCEEDED"):

            error_string = f"{run_details['error']['error']}"
            if(run_details['error'].get('cause')):
                error_string += run_details['error']['cause']

            _print_tabbed('error\t', error_string)

        _print_tabbed('start_date', run_details['startDate'])
        _print_tabbed('stop_date', run_details['stopDate'])
        _print_tabbed('completed_steps', run_details['completedSteps'])
        print()


def _pretty_print(result):

    WIDTH = 30

    print()
    print("=" * WIDTH , "EXPERIMENT", "=" * WIDTH, "\n")
    _format_item(result['info'])
    print()

    print("=" * WIDTH , "USERS", "=" * WIDTH, "\n")
    _format_table(result['users'])
    print()

    print("=" * WIDTH , "SAMPLES", "=" * WIDTH, "\n")
    if len(result['samples']):
        _format_table(result['samples'])
    else:
        print("No samples uploaded")
    print()

    print("=" * WIDTH , "PIPELINE RUNNER", "=" * WIDTH, "\n")
    _format_item(result['pipeline_runner'])
    print()

    print("=" * WIDTH , "RUNS", "=" * WIDTH, "\n")
    if len(result['runs']):
        _format_runs(result['runs'])
    else:
        print("Experiment has not been processed")


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
    "-p",
    "--aws_profile",
    required=False,
    default=DEFAULT_AWS_PROFILE,
    show_default=True,
    help="The name of the profile stored in ~/.aws/credentials to use.",
)
@click.option(
    "-o",
    "--output",
    required=False,
    default='false',
    show_default=True,
    help="Output result to a format. Supported format: json, yaml",
)
def info(
    experiment_id,
    input_env,
    aws_profile,
    output
):
    """
    Shows the required information related to the experiment.
    It requires an open tunnel to the desired environment to fetch data from SQL:
    `biomage rds tunnel -i production`

    E.g.:
    biomage experiment info -e 2093e95fd17372fb558b81b9142f230e -i production
    """

    aurora_client = AuroraClient(SANDBOX_ID, USER, REGION, input_env, aws_profile)

    aurora_client.open_tunnel()

    info = _get_experiment_info(aurora_client, experiment_id)

    pipeline_runner = {
        'pipeline_runner': "Batch" if info['pod_cpus'] else "Fargate",
        "pod_cpus": f"{info['pod_cpus'] } cores" if info['pod_cpus'] else "16 cores",
        "pod_memory": f"{info['pod_memory'] / 1024} GB" if info['pod_memory'] else "28 GB"
    }

    del info['pod_cpus']
    del info['pod_memory']

    users = _get_experiment_users(aurora_client, experiment_id, input_env)
    samples = _get_experiment_samples(aurora_client, experiment_id)
    runs = _get_experiment_runs(aurora_client, experiment_id)

    aurora_client.close_tunnel()

    result = {
        "info": info,
        "pipeline_runner": pipeline_runner,
        "users": users,
        "runs": runs,
        "samples": samples
    }

    if output == 'json':
        print(json.dumps(result))
        exit
    else:
        _pretty_print(result)

