import click
from configure_repo import configure_repo

@click.group()
def main():
    """🧬 Your one-stop shop for managing Biomage infrastructure."""

    pass

main.add_command(configure_repo.configure_repo)

if __name__ == "__main__":
    main()