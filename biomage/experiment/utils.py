import gzip
import json
import os
import time
from pathlib import Path

import boto3

COGNITO_STAGING_POOL = os.getenv("COGNITO_STAGING_POOL", "eu-west-1_mAQcge0PR")

DATA_LOCATION = os.getenv("BIOMAGE_DATA_PATH", "./data")


def get_experiment_project_id(experiment_id, source_table):

    table = boto3.resource("dynamodb").Table(source_table)

    project_id = table.get_item(
        Key={"experimentId": experiment_id}, ProjectionExpression="projectId"
    ).get("Item")["projectId"]

    return project_id
