from pathlib import Path
from typing import Annotated

from azure.ai.documentintelligence.models import (
    AnalyzeDocumentRequest,
    DocumentAnalysisFeature,
    DocumentContentFormat,
)

import az_ai.ingestion
from az_ai.ingestion import Document, DocumentIntelligenceResult
from az_ai.ingestion.helpers.documentation import markdown

ingestion = az_ai.ingestion.Ingestion()

ingestion.add_document_from_file("tests/data/test.pdf")

@ingestion.operation()
def apply_document_intelligence(
    document: Document,
) -> Annotated[DocumentIntelligenceResult, "document_intelligence_result"]:
    """
    Apply Document Intelligence to the document and return a fragment with the result.
    """
    poller = ingestion.document_intelligence_client.begin_analyze_document(
        model_id="prebuilt-layout",
        body=AnalyzeDocumentRequest(
            bytes_source=document.content,
        ),
        features=[
            DocumentAnalysisFeature.OCR_HIGH_RESOLUTION,
        ],
        output_content_format=DocumentContentFormat.Markdown,
    )
    return DocumentIntelligenceResult.with_source_result(
        document,
        label="document_intelligence_result",
        analyze_result=poller.result(),
    )

# Write the ingestor's diagram to a markdown file
Path("examples/doc.md").write_text(markdown(ingestion, "Sample Ingestor"))

# Run the ingestor
ingestion()