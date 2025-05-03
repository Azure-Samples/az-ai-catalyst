#!/usr/bin/env python3
import re
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
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    HnswAlgorithmConfiguration,
    HnswParameters,
    SearchableField,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SimpleField,
    VectorSearch,
    VectorSearchProfile,
)
from mlflow.entities import SpanType

import az_ai.ingestion
from az_ai.ingestion import Chunk, Document, DocumentIntelligenceResult, Fragment, ImageFragment
from az_ai.ingestion.repository import LocalRepository
from az_ai.ingestion.settings import IngestionSettings

# logging.basicConfig(level=logging.INFO)
# logging.getLogger("azure.core").setLevel(logging.WARNING)
# logging.getLogger("azure.identity").setLevel(logging.WARNING)

mlflow.openai.autolog()

class ItsaragSettings(IngestionSettings):
    model_name: str = "gpt-4.1-2025-04-14"
    index_name: str = "itsarag"
    temperature: float = 0.0
    max_tokens: int = 2000

    def as_params(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
    

settings = ItsaragSettings(repository_path="/tmp/itsarag_ingestion")

#
# Initialize Azure AI Foundry Services we will need
#

credential = DefaultAzureCredential()
project = AIProjectClient.from_connection_string(
    conn_str=settings.azure_ai_project_connection_string, credential=credential
)

document_intelligence_client = DocumentIntelligenceClient(
    endpoint=settings.azure_ai_endpoint,
    api_version="2024-11-30",
    credential=credential,
)

azure_openai_client = project.inference.get_azure_openai_client(
    api_version=settings.azure_openai_api_version,
)

search_index_client = SearchIndexClient(
    endpoint=settings.azure_ai_search_endpoint,
    credential=credential,
)

fields = [
    SimpleField(
        name="id",
        type=SearchFieldDataType.String,
        key=True,
        sortable=True,
        filterable=True,
        facetable=True,
        analyzer_name="keyword",
    ),
    SearchableField(
        name="content",
        type=SearchFieldDataType.String,
        searchable=True,
        analyzer_name="standard.lucene",
    ),
    SearchField(
        name="vector",
        type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
        hidden=True,
        searchable=True,
        filterable=False,
        sortable=False,
        facetable=False,
        vector_search_dimensions=3072,
        vector_search_profile_name="embedding_config",
    ),
    SimpleField(
        name="page_number",
        type=SearchFieldDataType.String,
        filterable=True,
        facetable=True,
    ),
    SimpleField(
        name="file_name",
        type=SearchFieldDataType.String,
        filterable=True,
        sortable=False,
        facetable=True,
    ),
    SimpleField(
        name="url",
        type=SearchFieldDataType.String,
        filterable=True,
        sortable=False,
        facetable=False,
    ),
    SimpleField(
        name="data_url",
        type="Edm.String",
        searchable=False,
        filterable=False,
        facetable=False,
        sortable=False,
    ),
]
index = SearchIndex(
    name=settings.index_name,
    fields=fields,
    vector_search=VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(
                name="hnsw_config",
                parameters=HnswParameters(metric="cosine"),
            )
        ],
        profiles=[
            VectorSearchProfile(
                name="embedding_config",
                algorithm_configuration_name="hnsw_config",
            ),
        ],
    ),
)


result = search_index_client.create_or_update_index(index=index)

search_client = SearchClient(
    endpoint=settings.azure_ai_search_endpoint,
    credential=credential,
    index_name=index.name,
)

#
# Ingestion workflow 
#

ingestion = az_ai.ingestion.Ingestion(settings=settings)

class Figure(ImageFragment):
    pass


class FigureDescription(Fragment):
    pass


class MarkdownFragment(Fragment):
    pass




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
            bytes_source=ingestion.repository.get(document).content,
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


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def extract_figures(
    di_result: DocumentIntelligenceResult,
) -> Annotated[list[Figure], "figure"]:
    """
    1. Process every figure in the "document_intelligence_result" fragment, extract the figure from
    its bounding box.
    2. Create a new image fragment for each figure.
    3. Insert a figure reference in the document_intelligence_result fragment Markdown.
    """
    from az_ai.ingestion.tools.markdown import MarkdownFigureExtractor

    return MarkdownFigureExtractor().extract(di_result, Figure)


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def describe_figure(
    image: ImageFragment,
) -> Annotated[FigureDescription, "figure_description"]:
    """
    1. Process the image fragment and generate a description.
    2. Create a new fragment with the description.
    """
    from az_ai.ingestion.tools.markdown import extract_code_block

    SYSTEM_CONTEXT = """\
        You are a helpful assistant that describe images in in vivid, precise details. 

        Focus on the graphs, charts, tables, and any flat images, providing clear descriptions of the data they 
        represent. 

        Specify the type of graphs (e.g., bar, line, pie), their axes, colors used, and any notable trends or patterns. 
        Mention the key figures, values, and labels.

        For each chart, describe how data points change over time or across categories, pointing out any significant 
        peaks, dips, or anomalies. If there are legends, footnotes, or annotations, detail how they contribute to 
        understanding the data.

        **IMPORTANT: Format your response as Markdown.**
    """

    response = azure_openai_client.chat.completions.create(
        model=settings.model_name,
        messages=[
            {"role": "system", "content": SYSTEM_CONTEXT},
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Describe this image.\n"
                            f"**Note**: the image has the following caption:\n {image.metadata['caption']})"
                        )
                        if "caption" in image.metadata
                        else "Describe this image.",
                    },
                    {"type": "image_url", "image_url": {"url": image.content_as_data_url()}},
                ],
            },
        ],
        temperature=settings.temperature,
        max_tokens=settings.max_tokens,
    )

    return FigureDescription.with_source(
        image,
        content=extract_code_block(response.choices[0].message.content)[0],
        mime_type="text/markdown",
        label="figure_description",
        update_metadata={
            "azure_openai_response": response.to_dict(),
            "data_url": image.content_as_data_url(),
        },
    )


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def split_markdown(
    document_intelligence_result: DocumentIntelligenceResult,
) -> Annotated[list[MarkdownFragment], "md_fragment"]:
    """
    1. Split the Markdown in the "document_intelligence_result" fragment into multiple fragments.
    2. Create a new Markdown fragment for each split.
    """
    from semantic_text_splitter import MarkdownSplitter

    MAX_CHARACTERS = 2000

    splitter = MarkdownSplitter(MAX_CHARACTERS, trim=False)

    figure_pattern = re.compile(r"<figure>.*?</figure>", re.DOTALL)
    page_break_pattern = re.compile(r"<!-- PageBreak -->")

    fragments = []
    page_nb = 1
    for i, chunk in enumerate(splitter.chunks(document_intelligence_result.content_as_str())):
        content = " ".join(figure_pattern.split(chunk))
        # TODO: this is a bit of a hack
        if page_break_pattern.match(content):
            page_nb += 1
        fragments.append(
            MarkdownFragment.with_source(
                document_intelligence_result,
                label="md_fragment",
                content=content,
                mime_type="text/markdown",
                human_index=i + 1,
                metadata={
                    "file_name": document_intelligence_result.metadata["file_name"],
                    "page_number": page_nb,
                },
            )
        )
    return fragments


@ingestion.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def embed(
    fragments: Annotated[list[Fragment], {"label": ["md_fragment", "figure_description"]}],
) -> Annotated[list[Chunk], "chunk"]:
    """
    For each figures or MD fragment create an chunk fragment
    """
    results = []
    for index, fragment in enumerate(fragments):
        response = azure_openai_client.embeddings.create(
            model="text-embedding-3-large",
            input=fragment.content_as_str(),
        )
        embedding = response.data[0].embedding
        results.append(
            Chunk.with_source(
                fragment,
                label="chunk",
                human_index=index + 1,
                content=fragment.content,
                vector=embedding,
                metadata={
                    "file_name": fragment.metadata["file_name"],
                    "page_number": fragment.metadata["page_number"],
                    "data_url": fragment.metadata.get("data_url", None),
                    "url": f"https://www.example.com/{fragment.metadata['file_name']}",
                },
            )
        )

    return results


# Write the ingestion pipeline diagram to a markdown file
with open("examples/itsarag.md", "w") as f:
    f.write("```mermaid\n---\ntitle: It's a RAG Ingestion Pipeline\n---\n")
    f.write(ingestion.mermaid())
    f.write("\n```")

# execute the ingestion pipeline

mlflow.set_experiment("itsarag")
with mlflow.start_run():
    mlflow.log_params(settings.as_params())
    # with mlflow.start_span("ingestion"):
    ingestion.add_document_from_file("tests/data/human-nutrition-2020-short.pdf")
    # ingestion.add_document_from_file("../itsarag/data/fsi/pdf/2023 FY GOOGL Short.pdf")
    # ingestion.add_document_from_file("../itsarag/data/fsi/pdf/2023 FY GOOGL.pdf")

    ingestion(search_client=search_client)


# Other ideas:
"""
from Document to document_intelligence_result:Fragment call apply_document_intelligence
from document_intelligence_result:Fragment to figures:Fragment call extract_figures
from figures:Fragment to text:Fragment call embedded
"""

# OR

# @ingestion.transformation(
#        "from Document to document_intelligence_result:Fragment" \
#        "use building_block document_intelligence"
#        )
# def apply_document_intelligence(content, )
