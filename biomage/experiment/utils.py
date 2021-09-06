import gzip
import json
import os
import time
from pathlib import Path

import boto3

from ..utils.constants import COGNITO_STAGING_POOL

DATA_LOCATION = os.getenv("BIOMAGE_DATA_PATH", "./data")
PULL = "PULL"


class Summary(object):
    """
    Utility singleton class used to report which files have been updated as a result of
    a given data management command.
    """

    changed_files = []
    cmd = ""
    origin = ""
    experiment_id = ""

    @classmethod
    def set_command(cls, cmd, origin, experiment_id):
        cls.cmd = cmd
        cls.origin = origin
        cls.experiment_id = experiment_id

    @classmethod
    def add_changed_file(cls, file):
        cls.changed_files.append(file)

    @classmethod
    def report_changes(cls):
        print(f"From {cls.origin}")
        print(f" * experiment {cls.experiment_id:^40s} -> {cls.cmd:>10s}")

        if len(cls.changed_files) < 1:
            print("Already up to date.")
            return

        print("Changes:")
        for file in cls.changed_files:
            print(f"{file:<70s} | Updated")


def save_cfg_file(dictionary, dst_file):
    local_file = os.path.join(DATA_LOCATION, dst_file)

    # try to create experiment folder, ignores if already exists (same as mkdir -p)
    Path(os.path.dirname(local_file)).mkdir(parents=True, exist_ok=True)

    with open(local_file, "w") as f:
        # We sort & indent the result to make it easier to inspect & debug the files
        # neither sorting nor indentation is used to check if two confis are equal
        json.dump(dictionary, f)


# If the config file was found => retun  (config file, true)
# Otherwise => (None, False)
def load_cfg_file(file):
    filepath = os.path.join(DATA_LOCATION, file)
    if os.path.exists(filepath) and not os.path.getsize(filepath) == 0:
        with open(os.path.join(DATA_LOCATION, file)) as f:
            return json.load(f), True

    return None, False


def set_modified_date(file_location, date):
    """
    Change the last-modified file parameter to date
    """
    mod_time = time.mktime(date.timetuple())

    os.utime(file_location, (mod_time, mod_time))


def get_local_S3_path(key):
    return os.path.join(DATA_LOCATION, key)


def is_modified(obj, key):
    """
    We check if the file in S3 has changed by comparing the last modified date
    which should be enough for our goals.
    Using E-tags would require either to download the file anyway to compute it or
    storing them in a local DB which seemed too complex. Moreover, there isn't a
    standard e-tag computation readily available and they can change among buckets,
    regions, etc...
    so it does not seem worth it.
    """
    local_file = get_local_S3_path(key)

    if not os.path.exists(local_file):
        return True

    if int(obj.last_modified.strftime("%s")) != int(os.path.getmtime(local_file)):
        return True

    return False


def download_S3_rds(s3_obj, key, filepath):
    local_file = get_local_S3_path(filepath)

    # try to create experiment folder, ignores if already exists (same as mkdir -p)
    Path(os.path.dirname(local_file)).mkdir(parents=True, exist_ok=True)

    with gzip.open(local_file, "wb") as f:
        f.write(s3_obj.get()["Body"].read())

    set_modified_date(file_location=local_file, date=s3_obj.last_modified)


def download_S3_json(s3_obj, key, filepath):
    local_file = get_local_S3_path(filepath)

    # try to create experiment folder, ignores if already exists (same as mkdir -p)
    Path(os.path.dirname(local_file)).mkdir(parents=True, exist_ok=True)

    s3_obj.download_file(local_file)

    set_modified_date(file_location=local_file, date=s3_obj.last_modified)


def get_cognito_username(email):
    client = boto3.client("cognito-idp")
    user_name = client.admin_get_user(Username=email, UserPoolId=COGNITO_STAGING_POOL)[
        "Username"
    ]

    return user_name


def get_experiment_project_id(experiment_id, source_table):

    table = boto3.resource("dynamodb").Table(source_table)

    project_id = table.get_item(
        Key={"experimentId": experiment_id}, ProjectionExpression="projectId"
    ).get("Item")["projectId"]

    return project_id


def add_user_to_rbac(user_name, cfg):
    if "rbac_can_write" in cfg:
        if user_name not in cfg["rbac_can_write"]["SS"]:
            cfg["rbac_can_write"]["SS"].append(user_name)
    for val in cfg.values():
        if isinstance(val, dict):
            add_user_to_rbac(user_name, val)


def add_env_user_to_experiment(cfg):
    email = os.getenv("BIOMAGE_EMAIL")
    if not email:
        raise ValueError(
            "biomage email not available to patch experiment permissions."
            + ' Set the environment variable "BIOMAGE_EMAIL" with the email you use to log in into cellscope'
            + " and try again."
        )

    user_name = get_cognito_username(email=email)

    add_user_to_rbac(user_name=user_name, cfg=cfg)

    return cfg
