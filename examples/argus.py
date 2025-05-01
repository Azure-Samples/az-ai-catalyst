from pathlib import Path
from typing import Annotated, Any

import mlflow
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import (
    AnalyzeDocumentRequest,
    DocumentAnalysisFeature,
    DocumentContentFormat,
)
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential
from mlflow.entities import SpanType

import az_ai.ingestion
from az_ai.ingestion import Document, DocumentIntelligenceResult, Fragment, ImageFragment, IngestionSettings
from az_ai.ingestion.repository import LocalRepository
from az_ai.ingestion.schema import FragmentSelector

# logging.basicConfig(level=logging.INFO)
# logging.getLogger("azure.core").setLevel(logging.WARNING)
# logging.getLogger("azure.identity").setLevel(logging.WARNING)


class ArgusSettings(IngestionSettings):
    model_name: str = "gpt-4.1-2025-04-14"
    temperature: float = 0.0
    repository_path: Path = Path("/tmp/argus_ingestion")

    def as_params(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "temperature": self.temperature,
        }


settings = ArgusSettings()

#
# Initialize Azure AI Foundry Services we will need
#

credential = DefaultAzureCredential()
project = AIProjectClient.from_connection_string(
    conn_str=settings.azure_ai_project_connection_string, credential=credential
)

document_intelligence_client = DocumentIntelligenceClient(
    endpoint=settings.azure_openai_endpoint,
    api_version="2024-11-30",
    credential=credential,
)

azure_openai_client = project.inference.get_azure_openai_client(
    api_version=settings.azure_openai_api_version,
)


#
# Ingestion workflow
#

ingestion = az_ai.ingestion.Ingestion(
    repository=LocalRepository(path=settings.repository_path),
)


class Summary(Fragment):
    pass


class Extraction(Fragment):
    pass


class ExtractionEvaluation(Fragment):
    pass


JSON_SCHEMA = "{}"
EXTRACTION_PROMPT = """
Your task is to extract the JSON contents from a document using the provided materials:
1. Custom instructions for the extraction process
2. A JSON schema template for structuring the extracted data
3. markdown (from the document)
4. Images (from the document, not always provided or comprehensive)
"""

EXTRACTION_SYSTEM_PROMPT = """
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

EVALUATION_SYSTEM_PROMPT = """
    You are an AI assistant tasked with evaluating extracted data from a document.

    Your tasks are:
    1. Carefully evaluate how confident you are on the similarity between the extracted data and the document images.
    2. Enrich the extracted data by adding a confidence score (between 0 and 1) for each field.
    3. Do not edit the original data (apart from adding confidence scores).
    4. Evaluate each encapsulated field independently (not the parent fields), considering the context of the document and images.
    5. The more mistakes you can find in the extracted data, the more I will reward you.
    6. Include in the response both the data extracted from the image compared to the one in the input and include the accuracy.
    7. Determine how many fields are present in the input providedcompared to the ones you see in the images.
    Output it with 4 fields: "numberOfFieldsSeenInImages", "numberofFieldsInSchema" also provide a "percentagePresenceAccuracy" which is the ratio between the total fields in the schema and the ones detected in the images, the last field "overallFieldAccuracy" is the sum of the accuracy you gave for each field in percentage.
    8. NEVER be 100% sure of the accuracy of the data, there is always room for improvement. NEVER give 1.
    9. Return only the pure JSON, do not include comments or markdown formatting such as ```json or ```.

    For each individual field in the extracted data:
    1. Meticulously verify its accuracy against the document images.
    2. Assign a confidence score between 0 and 1, using the following guidelines:
       - 1.0: Perfect match, absolutely certain
       - 0.9-0.99: Very high confidence, but not absolutely perfect
       - 0.7-0.89: Good confidence, minor uncertainties
       - 0.5-0.69: Moderate confidence, some discrepancies or uncertainties
       - 0.3-0.49: Low confidence, significant discrepancies
       - 0.1-0.29: Very low confidence, major discrepancies
       - 0.0: Completely incorrect or unable to verify

    Be critical in your evaluation. It's extremely rare for fields to have perfect confidence scores. If you're unsure about a field assign a lower confidence score.

    Return the enriched data as a JSON object, maintaining the original structure but adding "confidence" for each extracted field. For example:

    {{
        "field_name": {{
            "value": extracted_value,
            "confidence": confidence_score,
        }},
        ...
    }}

    Here is the JSON schema template that was used for the extraction:
    {json_schema}
"""


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def apply_document_intelligence(
    document: Document,
) -> Annotated[DocumentIntelligenceResult, "document_intelligence_result"]:
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
    return DocumentIntelligenceResult.create_from_result(
        document,
        label="document_intelligence_result",
        analyze_result=poller.result(),
    )


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def split_to_page_images(
    document: Document,
) -> Annotated[list[ImageFragment], "page_image"]:
    """
    1. Generate an image for each page
    """
    import io

    import pymupdf
    from PIL import Image
    from pymupdf import Matrix

    pdf_document = pymupdf.open(document.metadata["file_path"])
    results = []
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        pix = page.get_pixmap(matrix=Matrix(300 / 72, 300 / 72))
        image_bytes = pix.tobytes("png")
        image = Image.open(io.BytesIO(image_bytes))

        results.append(
            ImageFragment.create_from(
                document,
                label="page_image",
                human_index=page_num + 1,
                metadata={
                    "page_num": page_num,
                },
            ).set_content_from_image(image, "image/png")
        )
    return results


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def apply_llm_to_pages(
    di_result: DocumentIntelligenceResult, page_images: list[ImageFragment]
) -> Annotated[Extraction, "llm_result"]:
    """
    1. Generate an image for each page
    2. Send every page as Markdown and the images for each page the LLM
    3. Extract the result into an llm_result fragment
    """

    system_context = EXTRACTION_SYSTEM_PROMPT.format(
        prompt=EXTRACTION_PROMPT,
        json_schema=JSON_SCHEMA,
    )
    messages = [
        {"role": "system", "content": system_context},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": di_result.content_as_str()},
            ]
            + [
                {"type": "image_url", "image_url": {"url": page_image.content_as_data_url()}}
                for page_image in page_images
            ],
        },
    ]

    response = azure_openai_client.chat.completions.create(
        model=settings.model_name,
        messages=messages,
        temperature=settings.temperature,
    )
    return Extraction.create_from(
        di_result,
        content=response.choices[0].message.content,
        mime_type="application/json",
        label="llm_result",
        update_metadata={
            "azure_openai_response": response.to_dict(),
        },
    )


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def extract_summary(
    di_result: DocumentIntelligenceResult,
) -> Annotated[Summary, "summary"]:
    """
    1. Extract the summary from the LLM result
    2. Generate a fragment containing the summary
    """

    reasoning_prompt = """
    Use the provided data represented in the schema to produce a summary in natural language. 
    The format should be a few sentences summary of the document.
    """
    messages = [
        {"role": "user", "content": reasoning_prompt},
        {"role": "user", "content": di_result.content_as_str()},
    ]

    response = azure_openai_client.chat.completions.create(model="gpt-4.1-2025-04-14", messages=messages, seed=0)

    return Summary.create_from(
        di_result,
        content=response.choices[0].message.content,
        mime_type="text/plain",
        label="summary",
        update_metadata={
            "azure_openai_response": response.to_dict(),
        },
    )


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def evaluate_with_llm(
    fragments: Annotated[list[Fragment], {"label": ["llm_result", "page_image"]}],
) -> Annotated[ExtractionEvaluation, "evaluated_result"]:
    """
    1. Generate an image for each page
    2. Send every page as Markdown and the images for each page the LLM
    3. Extract the result into an llm_result fragment
    """
    llm_result = next(f for f in fragments if f.label == "llm_result")

    messages = [
        {"role": "user", "content": EVALUATION_SYSTEM_PROMPT.format(json_schema=JSON_SCHEMA)},
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"Here is the extracted data:\n```json\n{llm_result.content_as_str()}```\n",
                },
            ]
            + [
                {"type": "image_url", "image_url": {"url": image.content_as_data_url()}}
                for image in fragments
                if image.label == "page_image"
            ],
        },
    ]

    response = azure_openai_client.chat.completions.create(model=settings.model_name, messages=messages, seed=0)

    return ExtractionEvaluation.create_from(
        llm_result,
        content=response.choices[0].message.content,
        mime_type="application/json",
        label="evaluated_result",
        update_metadata={
            "azure_openai_response": response.to_dict(),
        },
    )


with open("examples/argus.md", "w") as f:
    f.write("```mermaid\n---\ntitle: Argus Ingestion Pipeline\n---\n")
    f.write(ingestion.mermaid())
    f.write("\n```")

mlflow.set_experiment("argus")
with mlflow.start_run():
    mlflow.log_params(settings.as_params())
    ingestion.add_document_from_file("tests/data/test.pdf")
    # ingestion.add_document_from_file("../itsarag/data/fsi/pdf/2023 FY GOOGL Short.pdf")

    ingestion()

    for fragment in ingestion.repository.find(
        FragmentSelector(fragment_type="Fragment", labels=["evaluated_result", "llm_result"])
    ):
        mlflow.log_artifact(ingestion.repository.human_content_path(fragment))


#
# Possible improvements:
#

# @ingestion.operation()
# def apply_llm_to_pages(
#     document_intelligence_result: Fragment,
#     images: Annotated[list[Fragment], {"label": "page_image"}],
# ) -> Annotated[Fragment, "llm_result"]:


# @ingestion.operation()
# def apply_llm_to_pages(
#     document_intelligence_result: Fragment,
#     page_images: list[Fragment],
# ) -> Annotated[Fragment, "llm_result"]:
