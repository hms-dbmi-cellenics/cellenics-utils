import json
import socket
from contextlib import closing

from ..rds.run import run_rds_command
from ..rds.tunnel import close_tunnel as close_tunnel_cmd
from ..rds.tunnel import open_tunnel as open_tunnel_cmd


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
            res = sock.connect_ex(('localhost', port))
            if res != 0:
                return port

class AuroraClient():
    def __init__(self, sandbox_id, user, region, env, aws_profile):
        self.sandbox_id = sandbox_id
        self.user = user
        self.region = region
        self.env = env
        self.aws_profile = aws_profile
        self.tunnel_port = None

    def open_tunnel(self):
        if (self.tunnel_port != None):
            return
    
        free_port = _find_free_port()
        if (free_port == None):
            raise Exception("No free port between 5432 and 6000")
        self.tunnel_port = free_port

        open_tunnel_cmd(self.env, self.region, self.sandbox_id, self.tunnel_port, self.aws_profile)

    def run_query(self, query):
        return run_rds_command(
            query,
            self.sandbox_id,
            self.env,
            self.user,
            self.region,
            self.aws_profile,
            local_port=self.tunnel_port,
            capture_output=True,
            verbose=False
        )

    def select(self, query, as_json = True):
        query = f"""psql -c "SELECT {"json_agg(q)" if as_json else "q" } FROM ( {query} ) AS q" """

        return _process_output_as_json(
            run_rds_command(
                query,
                self.sandbox_id,
                self.env,
                self.user,
                self.region,
                self.aws_profile,
                local_port=self.tunnel_port,
                capture_output=True,
                verbose=False
            )
        )

    def close_tunnel(self):
        close_tunnel_cmd()
        self.tunnel_port = None