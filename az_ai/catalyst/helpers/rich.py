from rich.console import Group
from rich.json import JSON
from rich.markdown import Markdown
from rich.markup import escape
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Column, Table, box
from rich.text import Text

from az_ai.catalyst.schema import Fragment


def fragment_as_table(fragment: Fragment) -> Panel:
    """
    Convert a Fragment to a Rich Panel.
    """

    metadata_table = Table(Column("Name", no_wrap=True), Column("Value", no_wrap=True), box=box.SIMPLE_HEAD)
    relationships_table = Table("Type", "Target", box=box.SIMPLE_HEAD)
    content_table = Table(box=box.SIMPLE_HEAD, show_header=False)

    tables = Table.grid()
    tables.add_column()
    tables.add_column()
    tables.add_row(metadata_table, relationships_table)

    for rel_type, target in fragment.relationships.items():
        relationships_table.add_row(str(rel_type.name), escape(str(target)))

    for key, value in fragment.metadata.items():
        value_str = str(value)
        value_str = escape(value_str[:100] + ("…" if len(value_str) > 100 else ""))
        metadata_table.add_row(Text(key), Text(value_str, overflow="ellipsis"))

    content_table.add_row("Content size", str(len(fragment.content)) if fragment.content else "0")
    content_table.add_row("mime type", fragment.mime_type)

    match fragment.class_name():
        case "Chunk":
            panel_box = box.HEAVY
        case "Document":
            panel_box = box.DOUBLE
        case _:
            panel_box = box.SQUARE

    if fragment.content is None:
        content = Text("Content is empty...", style="italic")
    else:
        match fragment.mime_type:
            case "text/markdown":
                content = fragment.content_as_str().splitlines()
                content = (content[:50] + ["\n__Truncated...__"]) if len(content) > 50 else content
                content = Markdown("\n".join(content))
            case "text/plain":
                content = Text(escape(fragment.content_as_str()), overflow="ellipsis")
            case "application/json":
                content = JSON(fragment.content_as_str())
            case mime if mime.startswith("image/"):
                content = Text(f"{mime} image placeholder...", style="italic")
            case _:
                content = Text(f"Unsupported {fragment.mime_type} placeholder...", style="italic")

    panel = Panel(
        Group(
            content,
            Rule(title="Content Details", style="white"),
            content_table,
            Rule(title="Metadata & Relationships", style="white"),
            tables,
        ),
        box=panel_box,
        title=f"[bold blue]{escape(str(fragment))}[/]",
    )
    return panel
