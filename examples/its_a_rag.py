import os
import logging
from pathlib import Path
from typing import Annotated

import dotenv

import az_ai.ingestion
from az_ai.ingestion.repository import LocalRepository
from az_ai.ingestion import Document, Fragment, Embedding, ImageFragment


dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO)



ingestion = az_ai.ingestion.Ingestion(
    repository=LocalRepository(path= Path("/tmp/its_a_rag_ingestion")),
)

@ingestion.operation()
def apply_document_intelligence(
    document: Document,
) -> Annotated[Fragment, "di_result"]:
    """
    Get the PDF and apply DocumentIntelligence
    Generate a fragment containing DocumentIntelligenceResult and Markdown
    """
    content = ingestion.repository.get_content(document)

    return Fragment.create_from(document, label="di_result")

@ingestion.operation()
def extract_figures(
    fragment: Annotated[Fragment, {"label": "di_result"}],
) -> Annotated[list[ImageFragment], "figure"]:
    """
    1. Process every figure in the "di_result" fragment, extract the figure from
    its bounding box.
    2. Create a new image fragment for each figure.
    3. Insert a figure reference in the di_result fragment Markdown.
    """
    return [
        ImageFragment.create_from(
            fragment, 
            label="figure", 
            metadata={"figure": "figure_1"})
    ]

@ingestion.operation()
def describe_figure(
    image: Annotated[ImageFragment, {"label": "figure"}],
) -> Annotated[Fragment, "figure_description"]:
    """
    1. Process the image fragment and generate a description.
    2. Create a new fragment with the description.
    """
    return Fragment.create_from(
        image, 
        label="figure_description",
        )


@ingestion.operation()
def split_markdown(
    fragment: Annotated[Fragment, {"label": "di_result"}],
) -> Annotated[list[Fragment], "md"]:
    """
    1. Split the Markdown in the "di_result" fragment into multiple fragments.
    2. Create a new Markdown fragment for each split.
    """
    return [
        Fragment.create_from(
            fragment, 
            label="md", 
            metadata={"md": "md_{i}"})
        for i in range(4)
    ]


@ingestion.operation()
def embed(
    fragment: Annotated[Fragment, {"label": ["md", "figure_description"]}],
) -> Embedding:
    """
    For each figures or MD fragment create an embedding fragment
    """
    return Embedding.create_from(fragment, vector=[0.1, 0.2, 0.3])


# Write the ingestion pipeline diagram to a markdown file
with open("examples/its_a_rag.md", "w") as f:
    f.write("```mermaid\n---\ntitle: It's a RAG Ingestion Pipeline\n---\n")
    f.write(ingestion.mermaid())
    f.write("\n```")

# execute the ingestion pipeline

ingestion.add_document_from_file("tests/data/test.pdf")

ingestion()


# Other ideas:
"""
from Document to di_result:Fragment call apply_document_intelligence
from di_result:Fragment to figures:Fragment call extract_figures
from figures:Fragment to text:Fragment call embedded
"""

# OR

#@ingestion.transformation(
#        "from Document to di_result:Fragment" \
#        "use building_block document_intelligence"
#        )
#def apply_document_intelligence(content, )
