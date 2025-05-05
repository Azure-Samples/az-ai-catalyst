from typing import Any

from az_ai.ingestion.ingestion import Ingestion
from az_ai.ingestion.schema import Chunk, Fragment


def create_embeddings(ingestion: Ingestion, model: str, fragment: Fragment, **kwargs: dict[str, Any]) -> Chunk:
    """
    Create embeddings for the fragment.

    Args:
        ingestion (Ingestion): The ingestion instance.
        model (str): The model to use for embeddings.
        fragment (Fragment): The fragment to create embeddings for.
        **kwargs: Additional arguments that will be passed to Chunk.with_source()

    Returns:
        Chunk: A new Chunk instance with the source set to the fragment, the vector set to the embeddings, 
        content set to the fragment's content and kwargs applied.
    """
    response = ingestion.azure_openai_client.embeddings.create(
                input=fragment.content_as_str(),
                model="text-embedding-3-large",
            )
    embedding = response.data[0].embedding

    return Chunk.with_source(
        fragment,
        content=fragment.content,
        vector=embedding,
        **kwargs,
    )
