import pytest

from az_ai.ingestion import Chunk, Fragment

METADATA = {
    "file_name": "test.pdf",
    "file_path": "/home/user/az-ai-ingestion/tests/data/test.pdf",
    "file_size": 92971,
    "mime_type": "chunk/pdf",
    "page_nb" : 1,
}


@pytest.fixture
def chunk():
    return Chunk(
        id="chunk",
        label="target",
        mime_type="text/markdown",
        metadata=dict(METADATA),
    )


def test_chunk_human_file_name(chunk):
    assert chunk.human_file_name() == "target.md"


def test_chunk_initialization(chunk):
    assert chunk.id == "chunk"
    assert chunk.label == "target"
    assert chunk.metadata == METADATA


def test_document_deserialization(chunk):
    document_json = """{ 
        "id": "chunk",
        "label": "png",
        "type": "Chunk",
        "metadata": {"key": "value"}
    }"""
    deserialized_chunk = Fragment.from_json(document_json)
    assert deserialized_chunk.id == chunk.id
    assert deserialized_chunk.label == "png"
    assert deserialized_chunk.metadata == {"key": "value"}

def test_document_deserialization_with_vector(chunk):
    document_json = """{ 
        "id": "chunk",
        "label": "pdf",
        "type": "Chunk", 
        "vector": [1, 2, 3, 4, 5]
    }"""
    deserialized_chunk = Fragment.from_json(document_json)
    assert deserialized_chunk.id == chunk.id
    assert deserialized_chunk.label == "pdf"
    assert deserialized_chunk.metadata == {}
    assert deserialized_chunk.vector == [1, 2, 3, 4, 5]