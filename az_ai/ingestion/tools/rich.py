from rich.console import Group
from rich.markup import escape

from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table, box, Column
from rich.text import Text


from az_ai.ingestion.schema import Fragment


def fragment_as_table(fragment: Fragment) -> Panel:
    """
    Convert a Fragment to a Rich Panel.
    """

    border_style = "white"
    metadata_table = Table(Column("Name"), Column("Value", no_wrap=True), box=box.SIMPLE_HEAD)
    relationships_table = Table(
        "Type", "Target", box=box.SIMPLE_HEAD
    )
    content_table = Table(box=box.SIMPLE_HEAD, show_header=False)

    # for rel_type, target in node.relationships.items():
    #     target_text = f"{target.node_type}\nNode ID: {target.node_id}\nFile name: {target.metadata.get('file_name', 'no filename')}"
    #     relationships_table.add_row(str(rel_type), target_text)

    relationships_table.add_row("Source", "Dummy")

    for key, value in fragment.metadata.items():
        value_str = escape(str(value))
        metadata_table.add_row(
            Text(key), Text(value_str, overflow="ellipsis") # value_str[:30] + ("…" if len(value_str) > 30 else ""), 
        )

    content_table.add_row(
        "content size", str(len(fragment.content)) if fragment.content else "0"
    )
    content_table.add_row("mime type", fragment.mime_type)

    content = "Text placeholder"

    panel = Panel(
        Group(
            content,
            Rule(style=border_style, title="Content Details"),
            content_table,
            Rule(style=border_style, title="Metadata & Relationships"),
            metadata_table,
            relationships_table,
        ),
        title=escape(str(fragment)),
        border_style=border_style,
    )
    return panel
