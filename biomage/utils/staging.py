import requests


def check_if_sandbox_exists(org, sandbox_id):
    url = f"https://raw.githubusercontent.com/{org}/releases/master/staging/{sandbox_id}.yaml"  # noqa: E501
    # url = f"https://raw.githubusercontent.com/{org}/flux-v2-migration/master/staging/{sandbox_id}.yaml"  # noqa: E501

    s = requests.Session()
    r = s.get(url)

    return 200 <= r.status_code < 300
