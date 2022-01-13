import click
from github import Github
from github.GithubException import UnknownObjectException
from PyInquirer import prompt


def configure(r, token):
    r.edit(
        private=False,
        has_issues=False,
        has_wiki=False,
        has_projects=False,
        allow_squash_merge=True,
        allow_merge_commit=False,
        allow_rebase_merge=False,
        delete_branch_on_merge=True,
        default_branch="master",
    )
    master = r.get_branch("master")
    master.edit_protection(
        user_push_restrictions=[],
        team_push_restrictions=[],
        required_approving_review_count=1,
        enforce_admins=True,
        strict=True,
        contexts=[],
    )
    master.add_required_signatures()

@click.command()
@click.argument("name")
@click.option(
    "--create",
    "-c",
    is_flag=True,
    show_default=True,
    help="Automatically create the repository if not already present.",
)
@click.option(
    "--token",
    "-t",
    envvar="GITHUB_API_TOKEN",
    required=True,
    show_default=True,
    help="A GitHub Personal Access Token with the required permissions.",
)
@click.option(
    "--org",
    envvar="GITHUB_BIOMAGE_ORG",
    default="hms-dbmi-cellenics",
    show_default=True,
    help="The GitHub organization to perform the operation in.",
)
def configure_repo(name, token, create, org):
    """
    Configures a repository to conform to standards.
    """

    g = Github(token)
    o = g.get_organization(org)

    click.echo(f"Successfully logged into organization {o.name} ({o.login}).")

    try:
        r = o.get_repo(name)
    except UnknownObjectException:
        if create:
            click.echo(f"Repository {name} does not exist, creating...")
        else:
            questions = [
                {
                    "type": "confirm",
                    "name": "create",
                    "message": f"Repository {name} does not exist. "
                    "Do you want to create it?",
                }
            ]

            answers = prompt(questions)

            if not answers["create"]:
                exit(1)

        r = o.create_repo(name)
        click.echo(f"✔️ {name} created.")
        click.echo(
            "✔️ Push something to `master` and re-run this utility to set "
            "everything up appropriately."
        )
        exit(0)

    configure(r, token)

    click.echo(f"✔️ Successfully configured {name} according to convention.")
