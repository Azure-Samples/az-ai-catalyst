#!/usr/bin/env python3
"""
"It's a RAG" Catalyst Example: demonstrate how the It's a RAG pattern can be implemented
with az-ai-catalyst.

Source: https://github.com/francesco-sodano/itsarag
"""

import re
from pathlib import Path
from textwrap import dedent
from typing import Annotated, Any

import mlflow
from azure.ai.documentintelligence.models import (
    AnalyzeDocumentRequest,
    DocumentAnalysisFeature,
    DocumentContentFormat,
)
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

import az_ai.catalyst
from az_ai.catalyst import Chunk, Document, DocumentIntelligenceResult, Fragment, ImageFragment
from az_ai.catalyst.helpers.documentation import markdown
from az_ai.catalyst.settings import CatalystSettings

# logging.basicConfig(level=logging.INFO)
# logging.getLogger("azure.core").setLevel(logging.WARNING)
# logging.getLogger("azure.identity").setLevel(logging.WARNING)

mlflow.openai.autolog()


class ItsaragSettings(CatalystSettings):
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


#
# Catalyst definition
#

catalyst = az_ai.catalyst.Catalyst(settings_cls=ItsaragSettings, repository_url="/tmp/itsarag_repo")

result = catalyst.search_index_client.create_or_update_index(
    index=SearchIndex(
        name=catalyst.settings.index_name,
        fields=[
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
        ],
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
)


class Figure(ImageFragment):
    pass


class FigureDescription(Fragment):
    pass


class MarkdownFragment(Fragment):
    pass


@catalyst.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def apply_document_intelligence(
    document: Document,
) -> Annotated[DocumentIntelligenceResult, "document_intelligence_result"]:
    """
    Get the PDF and apply DocumentIntelligence
    Generate a fragment containing DocumentIntelligenceResult and Markdown
    """

    poller = catalyst.document_intelligence_client.begin_analyze_document(
        model_id="prebuilt-layout",
        body=AnalyzeDocumentRequest(
            bytes_source=catalyst.repository.get(document).content,
        ),
        features=[DocumentAnalysisFeature.OCR_HIGH_RESOLUTION] if document.mime_type == "application/pdf" else [],
        output_content_format=DocumentContentFormat.Markdown,
    )
    return DocumentIntelligenceResult.with_source_result(
        document,
        label="document_intelligence_result",
        analyze_result=poller.result(),
    )


@catalyst.operation()
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
    from az_ai.catalyst.helpers.markdown import MarkdownFigureExtractor

    return MarkdownFigureExtractor().extract(di_result, Figure)


@catalyst.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def describe_figure(
    image: ImageFragment,
) -> Annotated[FigureDescription, "figure_description"]:
    """
    1. Process the image fragment and generate a description.
    2. Create a new fragment with the description.
    """
    from az_ai.catalyst.helpers.markdown import extract_code_block

    SYSTEM_CONTEXT = dedent("""\
        You are a helpful assistant that describe images in in vivid, precise details. 

        Focus on the graphs, charts, tables, and any flat images, providing clear descriptions of the data they 
        represent. 

        Specify the type of graphs (e.g., bar, line, pie), their axes, colors used, and any notable trends or patterns. 
        Mention the key figures, values, and labels.

        For each chart, describe how data points change over time or across categories, pointing out any significant 
        peaks, dips, or anomalies. If there are legends, footnotes, or annotations, detail how they contribute to 
        understanding the data.

        **IMPORTANT: Format your response as Markdown.**
    """)

    response = catalyst.azure_openai_client.chat.completions.create(
        model=catalyst.settings.model_name,
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
        temperature=catalyst.settings.temperature,
        max_tokens=catalyst.settings.max_tokens,
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


@catalyst.operation()
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


@catalyst.operation()
@mlflow.trace(span_type=SpanType.CHAIN)
def embed(
    fragments: Annotated[list[Fragment], {"label": ["md_fragment", "figure_description"]}],
) -> Annotated[list[Chunk], "chunk"]:
    """
    For each figures or MD fragment create an chunk fragment
    """
    from az_ai.catalyst.helpers.azure_openai import create_embeddings

    return [
        create_embeddings(
            catalyst=catalyst,
            fragment=fragment,
            label="chunk",
            human_index=index + 1,
            model="text-embedding-3-large",
            metadata={
                "file_name": fragment.metadata["file_name"],
                "page_number": fragment.metadata["page_number"],
                "data_url": fragment.metadata.get("data_url", None),
                "url": f"https://www.example.com/{fragment.metadata['file_name']}",
            },
        )
        for index, fragment in enumerate(fragments)
    ]


# Write the catalyst pipeline diagram to a markdown file
Path("examples/itsarag.md").write_text(markdown(catalyst, "It's a RAG Ingestor", description=__doc__))

# execute the catalyst pipeline
mlflow.set_experiment("itsarag")
with mlflow.start_run():
    mlflow.log_params(catalyst.settings.as_params())
    # with mlflow.start_span("catalyst"):
    catalyst.add_document_from_file("tests/data/human-nutrition-2020-short.pdf")
    # catalyst.add_document_from_file("../itsarag/data/fsi/pdf/2023 FY GOOGL Short.pdf")
    # catalyst.add_document_from_file("../itsarag/data/fsi/pdf/2023 FY GOOGL.pdf")

    catalyst()
    catalyst.update_index()
