from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from az_ai.ingestion.repository import LocalRepository
from az_ai.ingestion.schema import FragmentSelector
from az_ai.ingestion.tools.rich import fragment_as_table

app = typer.Typer(no_args_is_help=True)


@app.command()
def show(
    repository: Annotated[Path, typer.Option(help="Path to the repository.")],
    type: Annotated[str | None, typer.Option(help="Filter only fragment of this type.")] = None,
    label: Annotated[str | None, typer.Option(help="Filter only fragment of with this label.")] = None,
):
    console = Console()
    repository = LocalRepository(path=repository)
    if type or label:
        fragment_type = "Fragment" if type is None else type
        spec = FragmentSelector(fragment_type=fragment_type, labels=[label])
        result = repository.find(spec)
    else:
        result = repository.find()

    for fragment in result:
        console.print(fragment_as_table(fragment))


@app.command()
def human(
    repository: Annotated[Path, typer.Option(help="Path to the repository.")],
):
    if not repository.exists():
        print("Repository does not exist!")
        raise typer.Exit(code=1)

    repository = LocalRepository(path=repository)

    for path in sorted(repository.human_path().glob("**/*")):
        if path.is_file():
            print(path)


app()
