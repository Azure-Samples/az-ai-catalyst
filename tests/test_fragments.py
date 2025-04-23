import pytest

from az_ai.ingestion import Fragment, Document

@pytest.fixture
def fragment():
    return Fragment(
        id="fragment_1",
        label="md",
        metadata={"key": "value"},
    )


def test_fragment_initialization(fragment):
    assert fragment.id == "fragment_1"
    assert fragment.metadata == {"key": "value"}

def test_fragment_deserialization(fragment):
    fragment_json = """{ 
        "id": "fragment_1",
        "label": "md",
        "metadata": {"key": "value"},
        "type": "Fragment"
    }"""
    deserialized_fragment = Fragment.from_json(fragment_json)

    assert isinstance(deserialized_fragment, Fragment)
    assert deserialized_fragment.id == fragment.id
    assert deserialized_fragment.metadata == fragment.metadata

def test_document_deserialization(fragment):
    fragment_json = """{
        "id": "fragment_1",
        "label": "md",
        "metadata": {"key": "value"},
        "type": "Document"
    }"""
    deserialized_fragment = Fragment.from_json(fragment_json)

    assert isinstance(deserialized_fragment, Document)
    assert deserialized_fragment.id == fragment.id
    assert deserialized_fragment.metadata == fragment.metadata

def test_custom_sub_class_deserialization(fragment):
    class TestDocument(Document):
        pass

    fragment_json = """{
        "id": "fragment_1",
        "label": "md",
        "metadata": {"key": "value"},
        "type": "TestDocument"
    }"""
    deserialized_fragment = Fragment.from_json(fragment_json)

    assert isinstance(deserialized_fragment, TestDocument)
    assert deserialized_fragment.id == fragment.id
    assert deserialized_fragment.metadata == fragment.metadata