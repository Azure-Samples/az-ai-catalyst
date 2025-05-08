from typing import Any

from az_ai.catalyst.catalyst import Catalyst
from az_ai.catalyst.schema import Chunk, Fragment


def create_embeddings(catalyst: Catalyst, model: str, fragment: Fragment, **kwargs: dict[str, Any]) -> Chunk:
    """
    Create embeddings for the fragment.

    Args:
        catalyst (Catalyst): The catalyst instance.
        model (str): The model to use for embeddings.
        fragment (Fragment): The fragment to create embeddings for.
        **kwargs: Additional arguments that will be passed to Chunk.with_source()

    Returns:
        Chunk: A new Chunk instance with the source set to the fragment, the vector set to the embeddings, 
        content set to the fragment's content and kwargs applied.
    """
    response = catalyst.azure_openai_client.embeddings.create(
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
