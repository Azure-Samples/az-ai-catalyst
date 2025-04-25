import pytest

from az_ai.ingestion import Fragment, Document

@pytest.fixture
def document():
    return Document(
        id="doc_1",
        label="source",
        metadata={"key": "value"},
    )


def test_document_human_name(document):
    assert document.human_name() == "source/doc_1"

def test_document_initialization(document):
    assert document.id == "doc_1"
    assert document.label == "source"
    assert document.metadata == {"key": "value"}

def test_document_deserialization(document):
    document_json = """{ 
        "id": "doc_1",
        "label": "pdf",
        "type": "Document",
        "metadata": {"key": "value"}
    }"""
    deserialized_document = Fragment.from_json(document_json)
    assert deserialized_document.id == document.id
    assert deserialized_document.label == "pdf"
    assert deserialized_document.metadata == {"key": "value"}


def test_document_deserialization_with_url(document):
    document_json = """{ 
        "id": "doc_1",
        "label": "pdf",
        "type": "Document", 
        "content_url": "https://example.com/doc.pdf"
    }"""
    deserialized_document = Fragment.from_json(document_json)
    assert deserialized_document.id == document.id
    assert deserialized_document.label == "pdf"
    assert deserialized_document.metadata == {}
    assert deserialized_document.content_url == "https://example.com/doc.pdf"