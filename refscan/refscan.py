import typer
from typing_extensions import Annotated
from rich.console import Console
from pymongo import MongoClient, timeout

app = typer.Typer(
    help="Scan an NMDC MongoDB database for referential integrity issues.",
    add_completion=False,  # hides the shell completion options from `--help` output
    rich_markup_mode="markdown",  # enables use of Markdown in docstrings and CLI help
)

# Instantiate a Rich console for fancy console output.
# Reference: https://rich.readthedocs.io/en/stable/console.html
console = Console()


@app.command("scan")
def scan(
        database_name: Annotated[str, typer.Option(
            help="Name of the database.",
        )] = "nmdc",
        mongo_uri: Annotated[str, typer.Option(
            envvar="MONGO_URI",
            help="Connection string for accessing the MongoDB server. If you have Docker installed, "
                 "you can spin up a temporary MongoDB server at the default URI by running: "
                 "`$ docker run --rm --detach -p 27017:27017 mongo`",
        )] = "mongodb://localhost:27017",
):
    # Connect to MongoDB server.
    mongo_client: MongoClient = MongoClient(host=mongo_uri, directConnection=True)
    with (timeout(5)):  # if any message exchange takes > 5 seconds, this will raise an exception
        (host, port_number) = mongo_client.address
        console.print(f'Connected to MongoDB server: "{host}:{port_number}"')

        # Check whether the database exists on the MongoDB server.
        if database_name not in mongo_client.list_database_names():
            raise ValueError(f'Database "{database_name}" not found on the MongoDB server.')

    # Close the connection to the MongoDB server.
    mongo_client.close()


if __name__ == "__main__":
    app()
