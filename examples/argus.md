# Argus Ingestor
## Description


Argus Ingestion Example: demonstrate how the Argus pattern can be implemented 
with az-ai-ingestion.

Source: https://github.com/Azure-Samples/ARGUS

## Diagram
```mermaid
---
title: Argus Ingestor
---
flowchart TD
    Document@{ shape: doc, label: "Document[]" }

    apply_document_intelligence@{ shape: rect, label: "apply_document_intelligence" }
    split_to_page_images@{ shape: rect, label: "split_to_page_images" }
    extract_summary@{ shape: rect, label: "extract_summary" }
    apply_llm_to_pages@{ shape: rect, label: "apply_llm_to_pages" }
    evaluate_with_llm@{ shape: rect, label: "evaluate_with_llm" }

    DocumentIntelligenceResult_document_intelligence_result@{ shape: doc, label: "DocumentIntelligenceResult[document_intelligence_result]" }
    ImageFragment_page_image@{ shape: doc, label: "ImageFragment[page_image]" }
    Summary_summary@{ shape: doc, label: "Summary[summary]" }
    Extraction_extraction@{ shape: doc, label: "Extraction[extraction]" }
    ExtractionEvaluation_extraction_evaluation@{ shape: doc, label: "ExtractionEvaluation[extraction_evaluation]" }

    Document --> apply_document_intelligence
    apply_document_intelligence --> DocumentIntelligenceResult_document_intelligence_result

    Document --> split_to_page_images
    split_to_page_images -- \* --> ImageFragment_page_image

    DocumentIntelligenceResult_document_intelligence_result --> extract_summary
    extract_summary --> Summary_summary

    DocumentIntelligenceResult_document_intelligence_result --> apply_llm_to_pages
    ImageFragment_page_image -- \* --> apply_llm_to_pages
    apply_llm_to_pages --> Extraction_extraction

    Extraction_extraction --> evaluate_with_llm
    ImageFragment_page_image -- \* --> evaluate_with_llm
    evaluate_with_llm --> ExtractionEvaluation_extraction_evaluation

```
## Operations documentation
### apply_document_intelligence


Get the PDF and apply DocumentIntelligence
Generate a fragment containing DocumentIntelligenceResult and Markdown
<details>
<summary>Code</summary>

```python
@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def apply_document_intelligence(
    document: Document,
) -> Annotated[DocumentIntelligenceResult, "document_intelligence_result"]:
    """
    Get the PDF and apply DocumentIntelligence
    Generate a fragment containing DocumentIntelligenceResult and Markdown
    """
    poller = ingestion.document_intelligence_client.begin_analyze_document(
        model_id="prebuilt-layout",
        body=AnalyzeDocumentRequest(
            bytes_source=document.content,
        ),
        features=[DocumentAnalysisFeature.OCR_HIGH_RESOLUTION] if document.mime_type == "application/pdf" else [],
        output_content_format=DocumentContentFormat.Markdown,
    )
    return DocumentIntelligenceResult.with_source_result(
        document,
        label="document_intelligence_result",
        analyze_result=poller.result(),
    )

```

</details>

### split_to_page_images


1. Generate an image for each page
<details>
<summary>Code</summary>

```python
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

    pdf_document = pymupdf.open(stream=document.content, filetype="pdf")
    results = []
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        pix = page.get_pixmap(matrix=Matrix(300 / 72, 300 / 72))
        image_bytes = pix.tobytes("png")
        image = Image.open(io.BytesIO(image_bytes))

        results.append(
            ImageFragment.with_source(
                document,
                label="page_image",
                human_index=page_num + 1,
                metadata={
                    "page_num": page_num,
                },
            ).set_content_from_image(image, "image/png")
        )
    return results

```

</details>

### extract_summary


1. Extract the summary from the LLM result
2. Generate a fragment containing the summary
<details>
<summary>Code</summary>

```python
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

    response = ingestion.azure_openai_client.chat.completions.create(
        model="gpt-4.1-2025-04-14", messages=messages, seed=0
    )

    return Summary.with_source(
        di_result,
        content=response.choices[0].message.content,
        mime_type="text/plain",
        label="summary",
        update_metadata={
            "azure_openai_response": response.to_dict(),
            "document_intelligence_result": None,
        },
    )

```

</details>

### apply_llm_to_pages


1. Generate an image for each page
2. Send every page as Markdown and the images for each page the LLM
3. Extract the result into an Extraction fragment
<details>
<summary>Code</summary>

```python
@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def apply_llm_to_pages(
    di_result: DocumentIntelligenceResult, page_images: list[ImageFragment]
) -> Annotated[Extraction, "extraction"]:
    """
    1. Generate an image for each page
    2. Send every page as Markdown and the images for each page the LLM
    3. Extract the result into an Extraction fragment
    """

    system_context = EXTRACTION_SYSTEM_PROMPT.format(
        prompt=settings.extraction_prompt,
        json_schema=ingestion.settings.json_schema,
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

    response = ingestion.azure_openai_client.chat.completions.create(
        model=settings.model_name,
        messages=messages,
        temperature=settings.temperature,
    )
    return Extraction.with_source(
        di_result,
        content=response.choices[0].message.content,
        mime_type="application/json",
        label="extraction",
        update_metadata={
            "azure_openai_response": response.to_dict(),
            "document_intelligence_result": None,
        },
    )

```

</details>

### evaluate_with_llm


1. Generate an image for each page
2. Send every page as Markdown and the images for each page the LLM
3. Extract the result into an extraction fragment
<details>
<summary>Code</summary>

```python
@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def evaluate_with_llm(
    extraction: Extraction, page_images: list[ImageFragment]
) -> Annotated[ExtractionEvaluation, "extraction_evaluation"]:
    """
    1. Generate an image for each page
    2. Send every page as Markdown and the images for each page the LLM
    3. Extract the result into an extraction fragment
    """

    system_message = EVALUATION_SYSTEM_PROMPT.format(json_schema=ingestion.settings.json_schema)
    messages = [
        {"role": "system", "content": system_message},
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (f"Here is the extracted data:\n```json\n{extraction.content_as_str()}```\n"),
                },
            ]
            + [{"type": "image_url", "image_url": {"url": image.content_as_data_url()}} for image in page_images],
        },
    ]

    response = ingestion.azure_openai_client.chat.completions.create(
        model=settings.model_name, messages=messages, seed=0
    )

    return ExtractionEvaluation.with_source(
        extraction,
        content=response.choices[0].message.content,
        mime_type="application/json",
        label="extraction_evaluation",
        update_metadata={
            "azure_openai_response": response.to_dict(),
        },
    )

```

</details>
