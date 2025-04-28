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
    repository=LocalRepository(path=Path("/tmp/argus_ingestion")),
)


SYSTEM_PROMPT = """
Your task is to extract the JSON contents from a document using the provided materials:
1. Custom instructions for the extraction process
2. A JSON schema template for structuring the extracted data
3. markdown (from the document)
4. Images (from the document, not always provided or comprehensive)

Instructions:
- Use the markdown as the primary source of information, and reference the images for additional context and validation.
- Format the output as a JSON instance that adheres to the provided JSON schema template.
- If the JSON schema templatse is empty, create an appropriate structure based on the document content.
- If there are pictures, charts or graphs describe them in details in seperate fields (unless you have a specific JSON structure you need to follow).
- Return only the JSON instance filled with data from the document, without any additional comments (unless instructed otherwise).

Here are the Custom instructions you MUST follow:
```
{prompt}
```

Here is the JSON schema template:
```
{json_schema}
```
"""


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
            bytes_source=document.content,
        ),
        features=[
            DocumentAnalysisFeature.OCR_HIGH_RESOLUTION,
        ],
        output_content_format=DocumentContentFormat.Markdown,
    )
    result = poller.result()
    return Fragment.create_from(
        document,
        label="document_intelligence_result",
        mime_type="text/markdown",
        content=result.content,
        update_metadata={
            "document_intelligence_result": result.as_dict(),
        },
    )


@ingestion.operation()
def split_to_page_images(
    document: Document,
) -> Annotated[list[Fragment], "page_image"]:
    """
    1. Generate an image for each page
    """
    import pymupdf
    from pymupdf import Matrix
    from PIL import Image
    import io

    from az_ai.ingestion.tools.images import image_binary

    pdf_document = pymupdf.open(document.metadata["file_path"])
    results = []
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        pix = page.get_pixmap(matrix=Matrix(300 / 72, 300 / 72))  
        image_bytes = pix.tobytes("png")  
        image = Image.open(io.BytesIO(image_bytes))

        results.append(
            Fragment.create_from(
                document,
                label="page_image",
                mime_type="image/png",
                human_index=page_num + 1,
                content=image_binary(image, "image/png"),
                metadata={
                    "page_num": page_num,
                },
            )
        )
    return results


@ingestion.operation()
def apply_llm_to_pages(
    fragments: Annotated[list[Fragment], {"label": ["document_intelligence_result", "page_image"]}],
) -> Annotated[Fragment, "llm_result"]:
    """
    1. Generate an image for each page
    2. Send every page as Markdown and the images for each page the LLM
    3. Extract the result into an llm_result fragment
    """
    from az_ai.ingestion.tools.markdown import extract_code_block
    from az_ai.ingestion.tools.images import image_data_url

    doc_intel_fragment = next(f for f in fragments if f.label == "document_intelligence_result")
    system_context = SYSTEM_PROMPT.format(
        prompt="Extract all information",
        json_schema="{}",
    )
    messages = [
        {"role": "system", "content": system_context},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": doc_intel_fragment.content.decode("utf-8")},
            ] + [
                {"type": "image_url", "image_url": {"url": image_data_url(fragment.content, "image/png")}}
                for fragment in fragments
                if fragment.label == "page_image"
            ],
        },
    ]

    response = azure_openai_client.chat.completions.create(
        model="gpt-4.1-2025-04-14",
        messages=messages,
        temperature=0.0,
    )
    from pathlib import Path
    import json
    Path("/tmp/toto.txt").write_text(json.dumps(response.to_dict(), indent=2))
    return Fragment.create_from(
        doc_intel_fragment,
        content=response.choices[0].message.content,
        mime_type="text/markdown",
        label="llm_result",
        update_metadata={
            "azure_openai_response": response.to_dict(),
        },
    )


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
