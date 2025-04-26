import pytest

from az_ai.ingestion import Embedding, Fragment

METADATA = {
    "file_name": "test.pdf",
    "file_path": "/home/user/az-ai-ingestion/tests/data/test.pdf",
    "file_size": 92971,
    "mime_type": "embedding/pdf",
    "page_nb" : 1,
}


@pytest.fixture
def embedding():
    return Embedding(
        id="embedding-1",
        label="source",
        metadata=dict(METADATA),
    )


def test_embedding_human_file_name(embedding):
    assert embedding.human_file_name() == "source_embedding-1"


def test_embedding_initialization(embedding):
    assert embedding.id == "embedding-1"
    assert embedding.label == "source"
    assert embedding.metadata == METADATA


def test_document_deserialization(embedding):
    document_json = """{ 
        "id": "embedding-1",
        "label": "png",
        "type": "Embedding",
        "metadata": {"key": "value"}
    }"""
    deserialized_embedding = Fragment.from_json(document_json)
    assert deserialized_embedding.id == embedding.id
    assert deserialized_embedding.label == "png"
    assert deserialized_embedding.metadata == {"key": "value"}

def test_document_deserialization_with_vector(embedding):
    document_json = """{ 
        "id": "embedding-1",
        "label": "pdf",
        "type": "Embedding", 
        "vector": [1, 2, 3, 4, 5]
    }"""
    deserialized_embedding = Fragment.from_json(document_json)
    assert deserialized_embedding.id == embedding.id
    assert deserialized_embedding.label == "pdf"
    assert deserialized_embedding.metadata == {}
    assert deserialized_embedding.vector == [1, 2, 3, 4, 5]