import typer

app = typer.Typer(
    help="Scan a MongoDB database for referential integrity issues.",
    add_completion=False,  # hides the shell completion options from `--help` output
)


@app.command("my-command")
def main():
    print("Hello from refscan")


if __name__ == "__main__":
    app()
