import base64
import hashlib
import json
import math
import os
import re
from collections import namedtuple
from functools import reduce

import anybase32
import boto3
import click
import requests
import yaml
from botocore.exceptions import ClientError
from github import Github
from PyInquirer import prompt

REPOS = ("ui", "api", "pipeline", "worker")
ORG = "biomage-ltd"

def prompt_repos_to_release():
    questions = [    {
        "type": "checkbox",
        "name": "repos",
        "message": "Which repositories would you like to release?",
        "choices": [
            {"name": name, "checked": False}
            for name in REPOS
        ],
    }    ]
    click.echo()
    answers = prompt(questions)

    return answers["repos"]

def release_confirmed(repos):
    questions = [
        {
            "type": "confirm",
            "name": "create",
            "message": f"Create release version for repositories {repos}?",
            "default": False,
        }
    ]
    answers = prompt(questions)
    return answers["create"]

def get_release_workflow(token):
    g = Github(token)
    o = g.get_organization(ORG)
    r = o.get_repo("iac")

    wf = None
    for workflow in r.get_workflows():
        print(workflow.name)
        if workflow.name == "Create a new release":
            wf = str(workflow.id)

    return r.get_workflow(wf)
        
@click.command()
@click.option(
    "--token",
    "-t",
    envvar="GITHUB_API_TOKEN",
    required=True,
    help="A GitHub Personal Access Token with the required permissions.",
)
@click.option('--all', is_flag=True, help="Release all repositories")
def release(token, all):
    """
    Creates a new release.
    """

    repos = REPOS
    if not all:
        repos = prompt_repos_to_release()

    if len(repos) < 1:
        click.echo("No repositories selected, exiting release process.")
        exit(1)
    
    if not release_confirmed(repos):
        exit(1)


    wf = get_release_workflow(token)

    for repo in repos:
        wf.create_dispatch(
            ref="master",
            inputs={"repo": repo},
        )

        click.echo(
            click.style(
                f"✔️ Release creation for {repo} submitted. You can check your progress at "
                f"https://github.com/{ORG}/iac/actions",
                fg="green",
                bold=True,
            )
        )
