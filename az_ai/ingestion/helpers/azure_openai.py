from typing import Any

from az_ai.ingestion.ingestion import Ingestion
from az_ai.ingestion.schema import Chunk, Fragment


def create_embeddings(ingestion: Ingestion, model: str, fragment: Fragment, **kwargs: dict[str, Any]) -> list[float]:
    """
    Create embeddings for all fragments in the repository.
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
