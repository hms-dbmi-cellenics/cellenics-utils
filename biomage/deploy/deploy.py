import click
from github import Github
from PyInquirer import prompt
import boto3

"""
AWS_ACCESS_KEY_ID
Updated yesterday
AWS_SECRET_ACCESS_KEY
Updated yesterday
DEPLOY_KEY_ACCESS_TOKEN
Updated yesterday
"""


def do_deploy(org, region):
    repo = org.get_repo("iac")
    workflows = repo.get_workflows()
    deploy_wf = None

    for wf in workflows:
        if wf.path == ".github/workflows/deploy-infra.yaml":
            deploy_wf = wf

    if not deploy_wf:
        click.echo(
            click.style(
                "✖️ Could not find the deployment workflow in the repository.",
                fg="red",
                bold=True,
            )
        )

    resp = deploy_wf.create_dispatch("master", {"region": region})

    if not resp:
        click.echo(
            click.style(
                "✖️ Could not trigger the workflow. Try again.",
                fg="red",
                bold=True,
            )
        )
    else:
        click.echo(
            click.style(
                "✔️ Workflow dispatched. Check "
                f"https://github.com/${org.login}/iac/actions "
                "for live updates.",
                fg="green",
                bold=True,
            )
        )


@click.command()
@click.argument(
    "region",
    default="eu-west-1",
)
@click.option(
    "--token",
    "-t",
    envvar="GITHUB_API_TOKEN",
    required=True,
    help="A GitHub Personal Access Token with the required permissions.",
)
@click.option(
    "--org",
    envvar="GITHUB_BIOMAGE_ORG",
    default="biomage-ltd",
    help="The GitHub organization to perform the operation in.",
)
def deploy(region, org, token):
    """
    Deploy Biomage to an AWS account and region.
    """

    g = Github(token)
    o = g.get_organization(org)
    github_user = g.get_user()
    aws_user = boto3.resource("iam").CurrentUser()
    aws_account = boto3.client("sts").get_caller_identity().get("Account")

    click.echo(click.style("🔑 Credentials:", bold=True))
    click.echo(f"GitHub user: \t\t {github_user.name} ({github_user.login})")
    click.echo(f"AWS user: \t\t {aws_user.user_name} ({aws_user.user_id})")
    click.echo(f"AWS account: \t\t {aws_account}")
    click.echo(f"Region for deployment: \t {region}")
    click.echo()
    click.echo(click.style("🎯 Target:", bold=True))
    click.echo(f"GitHub organization: \t {o.name} ({o.login})")
    click.echo(f"Deployment repository: \t https://github.com/{o.login}/iac")

    click.echo(
        click.style(
            "❗ Carefully inspect the values shown above and ensure they are correct. "
            "The credentials shown above will be shared with the target.",
            fg="yellow",
        )
    )

    click.echo(
        click.style(
            "❗ Incorrect credentials may cause incurring AWS costs and potential "
            "downtime.",
            fg="yellow",
        )
    )

    click.echo(
        click.style(
            "❗ An incorrect target may cause credentials to leak to "
            "an untrusted repository.",
            fg="yellow",
        )
    )

    questions = [
        {
            "type": "confirm",
            "name": "create",
            "message": "Are you sure you want to (re)deploy Biomage? "
            "This operation cannot be undone.",
            "default": False,
        }
    ]

    create = prompt(questions)
    create = create["create"]

    if not create:
        click.echo("All right. Quitting...")
        exit(1)

    do_deploy(o, region)
