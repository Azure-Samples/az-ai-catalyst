import logging
from typing import Annotated

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment

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
def apply_llm_to_pages(
    fragment: Annotated[Fragment, {"label": "di_result"}],
) -> Annotated[Fragment, "llm_result"]:
    """
    1. Generate an image for each page
    2. Send every page as Markdown and the images for each page the LLM
    3. Extract the result into an llm_result fragment
    """
    pass


@ingestion.operation()
def evaluate_with_llm(
    fragment: Annotated[Fragment, {"label": "llm_result"}],
) -> Annotated[Fragment, "evaluated_result"]:
    """
    1. Generate an image for each page
    2. Send every page as Markdown and the images for each page the LLM
    3. Extract the result into an llm_result fragment
    """
    pass


# TODO

with open("examples/argus.md", "w") as f:
    f.write("```mermaid\n---\ntitle: Argus Ingestion Pipeline\n---\n")
    f.write(ingestion.mermaid())
    f.write("\n```")


ingestion(file="example.pdf")
