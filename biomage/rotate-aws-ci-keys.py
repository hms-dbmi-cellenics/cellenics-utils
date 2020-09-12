# Creates, rotates, and updates GitHub repositories with the appropriate
# AWS access rights.

from functools import reduce
import boto3
from github import Github
import os
import yaml
import requests
from base64 import b64encode
from nacl import encoding, public

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
BIOMAGE_ORG = "biomage-ltd"


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def encrypt(public_key, secret_value):
    public_key = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
    sealed_box = public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return b64encode(encrypted).decode("utf-8")


def filter_iam_repos(repo):
    if repo.archived:
        return False

    # get files in root
    contents = repo.get_contents("")

    for content in contents:
        # search for tags.y(a)ml file
        if content.path != "tags.yml" and content.path != "tags.yaml":
            continue

        # open contents
        tags = yaml.safe_load(content.decoded_content)

        if recursive_get(tags, "ci", "iam-user"):
            return repo.name, recursive_get(tags, "ci", "iam-role")

        return False

    return False


def create_new_iam_users(iam, roles):
    users_created = 0
    print("Now creating IAM users for each repository...")
    for repo in roles:
        try:
            iam.create_user(
                Path=f"/githubactions/{repo}/",
                UserName=f"github-actions-{repo}",
                Tags=[
                    {"Key": "type", "Value": "ci-iam-user"},
                    {"Key": "repo", "Value": repo},
                ],
            )
            users_created += 1
        except iam.exceptions.EntityAlreadyExistsException:
            pass

    print("Created", users_created, "new users.")


def create_new_access_keys(iam, roles):
    print("Now creating new access keys for users...")
    keys = {}

    for repo in roles:
        key = iam.create_access_key(UserName=f"github-actions-{repo}")
        keys[repo] = (
            key["AccessKey"]["AccessKeyId"],
            key["AccessKey"]["SecretAccessKey"],
        )

    return keys


def update_github_secrets(keys):
    print("Now updating all repositories with new keys...")

    s = requests.Session()
    s.headers = {"Authorization": f"token {GITHUB_TOKEN}", "User-Agent": "Requests"}
    url_base = f"https://api.github.com/repos/{BIOMAGE_ORG}"

    results = {}

    for repo_name, (access_key_id, secret_access_key) in keys.items():
        ci_keys = s.get(f"{url_base}/{repo_name}/actions/secrets/public-key")

        if ci_keys.status_code != requests.codes.ok:
            results[repo_name] = ci_keys.status_code
            continue

        ci_keys = ci_keys.json()

        access_key_id = encrypt(ci_keys["key"], access_key_id)
        secret_access_key = encrypt(ci_keys["key"], secret_access_key)

        r = s.put(
            f"{url_base}/{repo_name}/actions/secrets/AWS_ACCESS_KEY_ID",
            json={"encrypted_value": access_key_id, "key_id": ci_keys["key_id"]},
        )

        r = s.put(
            f"{url_base}/{repo_name}/actions/secrets/AWS_SECRET_ACCESS_KEY",
            json={"encrypted_value": secret_access_key, "key_id": ci_keys["key_id"]},
        )

        results[repo_name] = r.status_code

    return results


def rollback_if_necessary(iam, keys, result_codes):
    print("Results for each repository:")

    print(
        "{0:<15}{1:<25}{2:<15}".format("REPOSITORY", "UPDATE STATUS (HTTP)", "STATUS")
    )
    for repo, code in result_codes.items():

        status = None
        username = f"github-actions-{repo}"
        generated_key_id, _ = keys[repo]

        if not 200 <= code <= 299:
            iam.delete_access_key(UserName=username, AccessKeyId=generated_key_id)
            status = "Key rolled back"
        else:
            user_keys = iam.list_access_keys(UserName=username)
            user_keys = user_keys["AccessKeyMetadata"]

            keys_deleted = 0

            for key in user_keys:
                if key["AccessKeyId"] == generated_key_id:
                    continue

                iam.delete_access_key(UserName=username, AccessKeyId=key["AccessKeyId"])
                keys_deleted += 1

            status = f"Removed {keys_deleted} old keys"

        print(f"{repo:<15}{code:<25}{status:<15}")


def main():
    print("Logging into GitHub and getting all repositories...")

    g = Github(GITHUB_TOKEN)
    org = g.get_organization(BIOMAGE_ORG)
    repos = org.get_repos()

    print(
        "Found",
        repos.totalCount,
        "repositories, finding ones with required CI privileges...",
    )

    roles = [ret for ret in (filter_iam_repos(repo) for repo in repos) if ret]
    print("Found", len(roles), "repositories marked as requiring CI IAM roles.")
    roles = dict(roles)

    iam = boto3.client("iam")
    create_new_iam_users(iam, roles)
    keys = create_new_access_keys(iam, roles)

    result_codes = update_github_secrets(keys)

    rollback_if_necessary(iam, keys, result_codes)


if __name__ == "__main__":
    main()
