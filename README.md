# AZ AI Ingestion Processor (Experimental)

Experimental __Ingestion framework__ to make it easier to build ingestion pipelines for Azure OpenAI Services.

## What is it?

The ingestion processor is a framework to build ingestion pipelines for Azure OpenAI Services. It allows you to define a series of operations that can be applied to documents, and it handles the execution of those operations in the correct order.

The ingestion processor is designed to be flexible and extensible, allowing you to define your own operations and customize the behavior of the framework to suit your needs.

## Why use it?

Ultimately we want this framework to:

- Designed for simplicity and ease of use
- Highly flexible and customizable
- Includes native integration with Azure OpenAI Services
- Automatically resumes data ingestion from the point of failure
- Offers built-in tools for managing settings
- Provides a comprehensive set of ready-to-use operations


> [!WARNING]
> Right now operations are executed in order of appearance in the code. Calculating dependencies
> between operations is not supported yet. This means that if you have a complex graph you 
> have to be careful ensuring that fragments needed for subsequent operations are already processed before 
> they are used.

## Quick Start

### Configure the environment

Copy the `env.example` file to `.env` and set the environment variables.

Alternatively you can provide the environment variables directly in the command line or through your shell.

### Run the examples

#### It's a RAG example

```bash
uv run examples/itsarag.py
```

> [!NOTE]
> `itsarag.py` also generates a Mermaid diagram of its graph in [examples/itsarag.md](examples/itsarag.md).

To visualize the processed fragments you can then use the AZ AI Ingestion CLI:

```bash
uv run az-ai-ingestion show --repository /tmp/itsarag_ingestion/
```

#### Argus example

```bash
uv run examples/argus.py
```

> [!NOTE]  
> `argus.py` also generates a Mermaid diagram of its graph in [examples/argus.md](examples/argus.md).

To visualize the processed fragments you can then use the AZ AI Ingestion CLI:

```bash
uv run az-ai-ingestion show --repository /tmp/argus_ingestion/
```

### Run the tests

```bash
uv run pytest
```

### Visualize the traces (experimental)

```bash
uv run mlflow ui --port 5000
```

Then open your browser and go to [http://localhost:5000](http://localhost:5000).

### Simple Custom Processor Example

Here is a simple example of how to use the ingestion processor.
```python
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
```

To run the above example run the following command after populating your `.env` file:

```bash
REPOSITORY_PATH=/tmp/repository uv run examples/doc.py
```

The documentation will be generated in [examples/doc.md](examples/doc.md).

## Responsible AI Guidelines

This project follows responsible AI guidelines and best practices, please review them before using this project:

- [Microsoft Responsible AI Guidelines](https://www.microsoft.com/en-us/ai/responsible-ai)
- [Responsible AI practices for Azure OpenAI models](https://learn.microsoft.com/en-us/legal/cognitive-services/openai/overview)
- [Safety evaluations transparency notes](https://learn.microsoft.com/en-us/azure/ai-studio/concepts/safety-evaluations-transparency-note)

## Inspired by

- [Argus](https://github.com/dbroeglin/ARGUS)
- [It's a RAG](https://github.com/francesco-sodano/itsarag)
- [Multimodal Document Processing](https://github.com/samelhousseini/mm_doc_proc)
- [Azure Multimodal AI + LLM Processing Accelerator](https://github.com/Azure/multimodal-ai-llm-processing-accelerator)

## Authors

  * [Dominique Broeglin](https://github.com/dbroeglin)
  * [Evgeny Minkevich](https://github.com/evmin)