import logging
from typing import Annotated

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment, SearchDocument

logging.basicConfig(level=logging.INFO)

ingestion = az_ai.ingestion.Ingestion()


@ingestion.operation()
def apply_document_intelligence(
    document: Document,
) -> Annotated[Fragment, "di_result"]:
    """
    Get the PDF and apply DocumentIntelligence
    Generate a fragment containing DocumentIntelligenceResult and Markdown
    """
    pass


@ingestion.operation()
def extract_figures(
    fragment: Annotated[Fragment, {"label": "di_result"}],
) -> Annotated[list[Fragment], "figure"]:
    """
    1. Process every figure in the "di_result" fragment, extract the figure from
    its bounding box.
    2. Create a new image fragment for each figure.
    3. Insert a figure reference in the di_result fragment Markdown.
    """
    pass


@ingestion.operation()
def split_markdown(
    fragment: Annotated[Fragment, {"label": "di_result"}],
) -> Annotated[list[Fragment], "md"]:
    """
    1. Split the Markdown in the "di_result" fragment into multiple fragments.
    2. Create a new Markdown fragment for each split.
    """
    pass


@ingestion.operation()
def embedded(
    fragment: Annotated[Fragment, {"label": ["md", "figure"]}],
) -> SearchDocument:
    """
    For each figures or MD fragment create an embedding fragment
    """
    pass



with open("examples/its_a_rag.md", "w") as f:
    f.write("```mermaid\n---\ntitle: It's a RAG Ingestion Pipeline\n---\n")
    f.write(ingestion.mermaid())
    f.write("\n```")

# execute the ingestion pipeline
ingestion(file="example.pdf")


# Other ideas:
"""
from Document to di_result:Fragment call apply_document_intelligence
from di_result:Fragment to figures:Fragment call extract_figures
from figures:Fragment to text:Fragment call embedded
"""

# OR

# @ingestion.transformation("from Document to di_result:Fragment")
