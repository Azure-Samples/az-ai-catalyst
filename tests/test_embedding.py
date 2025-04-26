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
        id="embedding",
        label="target",
        mime_type="text/markdown",
        metadata=dict(METADATA),
    )


def test_embedding_human_file_name(embedding):
    assert embedding.human_file_name() == "target.md"


def test_embedding_initialization(embedding):
    assert embedding.id == "embedding"
    assert embedding.label == "target"
    assert embedding.metadata == METADATA


def test_document_deserialization(embedding):
    document_json = """{ 
        "id": "embedding",
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
        "id": "embedding",
        "label": "pdf",
        "type": "Embedding", 
        "vector": [1, 2, 3, 4, 5]
    }"""
    deserialized_embedding = Fragment.from_json(document_json)
    assert deserialized_embedding.id == embedding.id
    assert deserialized_embedding.label == "pdf"
    assert deserialized_embedding.metadata == {}
    assert deserialized_embedding.vector == [1, 2, 3, 4, 5]