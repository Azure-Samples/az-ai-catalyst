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


# TODO

ingestion(file="example.pdf")
