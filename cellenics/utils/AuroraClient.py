import json
import socket
import sys
from contextlib import closing
from subprocess import run as sub_run

import boto3

from ..rds.tunnel import close_tunnel as close_tunnel_cmd
from ..rds.tunnel import open_tunnel as open_tunnel_cmd

# we use writer because reader might also point to writer making it not safe
ENDPOINT_TYPE = "writer"


def _run_rds_command(
    command,
    sandbox_id,
    input_env,
    user,
    region,
    aws_profile,
    local_port=None,
    capture_output=False,
    verbose=True,
):
    aws_session = boto3.Session(profile_name=aws_profile, region_name=region)

    password = None

    if input_env == "development":
        password = "password"
        local_port = local_port or 5431
    else:
        local_port = local_port or 5432

        rds_client = aws_session.client("rds")

        remote_endpoint = _get_rds_endpoint(
            input_env, sandbox_id, rds_client, ENDPOINT_TYPE
        )

        if verbose:
            print(
                f"Generating temporary token for {input_env}-{sandbox_id}",
                file=sys.stderr,
            )

        password = rds_client.generate_db_auth_token(
            remote_endpoint, 5432, user, region
        )

    if verbose:
        print("Token generated", file=sys.stderr)

    result = None

    if capture_output:
        result = sub_run(
            f'PGPASSWORD="{password}" {command} \
                --host=localhost \
                --port={local_port} \
                --username={user} \
                --dbname=aurora_db',
            capture_output=True,
            text=True,
            shell=True,
        )
    else:
        result = sub_run(
            f'PGPASSWORD="{password}" {command} \
                --host=localhost \
                --port={local_port} \
                --username={user} \
                --dbname=aurora_db',
            shell=True,
        )

    if result.returncode != 0:
        raise Exception(result.stderr)

    if capture_output:
        return result.stdout


def _get_rds_endpoint(input_env, sandbox_id, rds_client, endpoint_type):
    response = rds_client.describe_db_cluster_endpoints(
        DBClusterIdentifier=f"aurora-cluster-{input_env}-{sandbox_id}",
        Filters=[
            {"Name": "db-cluster-endpoint-type", "Values": [endpoint_type]},
        ],
    )

    return response["DBClusterEndpoints"][0]["Endpoint"]


def _process_output_as_json(query_result):
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


def _find_free_port():
    for port in range(5432, 6000):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            res = sock.connect_ex(("localhost", port))
            if res != 0:
                return port


class AuroraClient:
    def __init__(self, sandbox_id, user, region, env, aws_profile, local_port=None):
        self.sandbox_id = sandbox_id
        self.user = user
        self.region = region
        self.env = env
        self.aws_profile = aws_profile
        self.local_port = local_port

    def __enter__(self):
        self.open_tunnel()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close_tunnel()

    def open_tunnel(self):
        if self.local_port is None:
            free_port = _find_free_port()
            if free_port is None:
                raise Exception("No free port between 5432 and 6000")
            self.local_port = free_port

        open_tunnel_cmd(
            self.env, self.region, self.sandbox_id, self.local_port, self.aws_profile
        )

    def run_query(self, query, capture_output=True, verbose=False):
        return _run_rds_command(
            query,
            self.sandbox_id,
            self.env,
            self.user,
            self.region,
            self.aws_profile,
            local_port=self.local_port,
            capture_output=capture_output,
            verbose=verbose,
        )

    def select(self, query, as_json=True):
        query = f"""psql -c "SELECT {"json_agg(q)" if as_json else "q" }
                             FROM ( {query} ) AS q" """

        return _process_output_as_json(
            _run_rds_command(
                query,
                self.sandbox_id,
                self.env,
                self.user,
                self.region,
                self.aws_profile,
                local_port=self.local_port,
                capture_output=True,
                verbose=False,
            )
        )

    def close_tunnel(self):
        close_tunnel_cmd()
        self.local_port = None
