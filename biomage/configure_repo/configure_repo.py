import click
from github import Github

@click.command()
@click.argument('name')
@click.option('--create', '-c', is_flag=True, help='Automatically create the repository if not already present.')
@click.option('--token', '-t', envvar='GITHUB_API_TOKEN', required=True, help="A GitHub Personal Access Token with the required permissions.")
              
def configure_repo(name, token, create):
    """
    Configures a repository to conform to standards.
    """

    print('wowza', name, create)
    pass
