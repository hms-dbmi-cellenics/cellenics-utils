import json

from ..rds.run import run_rds_command


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

def init_db(SANDBOX_ID, USER, REGION, env, aws_profile):
    def _query_db(query):
        query = f"""psql -c "SELECT json_agg(q) FROM ( {query} ) AS q" """

        return _process_query_output(
            run_rds_command(
                query,
                SANDBOX_ID,
                env,
                USER,
                REGION,
                aws_profile,
                capture_output=True,
                verbose=False
            )
        )

    return _query_db