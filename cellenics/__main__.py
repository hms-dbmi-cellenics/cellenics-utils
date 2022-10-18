import click

from cellenics.account import account
from cellenics.configure_repo import configure_repo
from cellenics.experiment import experiment
from cellenics.rds import rds
from cellenics.rotate_ci import rotate_ci
from cellenics.stage import stage
from cellenics.unstage import unstage


@click.group()
def main():
    """🧬 Your one-stop shop for managing Cellenics infrastructure."""


main.add_command(configure_repo.configure_repo)
main.add_command(rotate_ci.rotate_ci)
main.add_command(stage.stage)
main.add_command(unstage.unstage)
main.add_command(experiment.experiment)
main.add_command(account.account)
main.add_command(rds.rds)

if __name__ == "__main__":
    main()
