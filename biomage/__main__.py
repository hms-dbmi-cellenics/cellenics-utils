import click
from configure_repo import configure_repo
from rotate_ci import rotate_ci
from stage import stage
from unstage import unstage
from experiment import experiment
from release import release


@click.group()
def main():
    """🧬 Your one-stop shop for managing Biomage infrastructure."""

    pass


main.add_command(configure_repo.configure_repo)
main.add_command(rotate_ci.rotate_ci)
main.add_command(stage.stage)
main.add_command(unstage.unstage)
main.add_command(experiment.experiment)
main.add_command(release.release)

if __name__ == "__main__":
    main()
