import logging
import os
from pathlib import Path
from typing import Annotated

import dotenv
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import (
    AnalyzeDocumentRequest,
    AnalyzeResult,
    DocumentAnalysisFeature,
    DocumentContentFormat,
)
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential
from azure.search.documents.indexes import SearchIndexClient

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment
from az_ai.ingestion.repository import LocalRepository

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO)
logging.getLogger("azure.core").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)

#
# Initialize Azure AI Foundry Services we will need
#

credential = DefaultAzureCredential()
project = AIProjectClient.from_connection_string(
    conn_str=os.getenv("AZURE_AI_PROJECT_CONNECTION_STRING"), credential=credential
)

document_intelligence_client = DocumentIntelligenceClient(
    endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version="2024-11-30",
    credential=credential,
)

azure_openai_client = project.inference.get_azure_openai_client(
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
)

search_index_client = SearchIndexClient(
    endpoint=os.getenv("AZURE_AI_SEARCH_ENDPOINT"),
    credential=credential,
)

#
# Ingestion workflow
#

ingestion = az_ai.ingestion.Ingestion(
    repository=LocalRepository(path=Path("/tmp/argus")),
)


@ingestion.operation()
def apply_document_intelligence(
    document: Document,
) -> Annotated[Fragment, "document_intelligence_result"]:
    """
    Get the PDF and apply DocumentIntelligence
    Generate a fragment containing DocumentIntelligenceResult and Markdown
    """
    poller = document_intelligence_client.begin_analyze_document(
        model_id="prebuilt-layout",
        body=AnalyzeDocumentRequest(
            bytes_source=ingestion.repository.get_content(document),
        ),
        features=[
            DocumentAnalysisFeature.OCR_HIGH_RESOLUTION,
        ],
        output_content_format=DocumentContentFormat.Markdown,
    )

    return Fragment.create_from(
        document,
        label="document_intelligence_result",
        mime_type="text/markdown",
        update_metadata={
            "document_intelligence_result": poller.result().as_dict(),
        },
    )


@ingestion.operation()
def apply_llm_to_pages(
    fragment: Annotated[Fragment, {"label": "document_intelligence_result"}],
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


ingestion.add_document_from_file("../itsarag/data/fsi/pdf/2023 FY GOOGL Short.pdf")

ingestion()
