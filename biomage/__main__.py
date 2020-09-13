import click
from configure_repo import configure_repo
from rotate_ci import rotate_ci


@click.group()
def main():
    """🧬 Your one-stop shop for managing Biomage infrastructure."""

    pass


main.add_command(configure_repo.configure_repo)
main.add_command(rotate_ci.rotate_ci)


if __name__ == "__main__":
    main()
