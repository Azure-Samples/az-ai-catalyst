from pathlib import Path

import pytest

from az_ai.ingestion import Document, Fragment, FragmentRelationships


@pytest.fixture
def fragment():
    return Fragment(
        id="fragment_1",
        label="md",
        metadata={"key": "value"},
        relationships={
            FragmentRelationships.SOURCE: "source",
            FragmentRelationships.SOURCE_DOCUMENT: "source_document"
        }
    )


@pytest.mark.parametrize(
    "label,mime_type,parents,human_index,expected",
    [
        ("di", "application/octet-stream", ["test"], None, "test/di.bin"),
        ("figure", "image/png", [], None, "figure.png"),
        ("figure", "image/png", ["test"], None, "test/figure.png"),
        ("figure", "image/png", ["test"], 1, "test/figure_001.png"),
        ("figure", "image/png", ["test"], 2, "test/figure_002.png"),
    ],
)
def test_fragment_human_file_name_with_elements(
    label, mime_type, parents, human_index, expected
):
    fragment = Fragment(
        id="fragment",
        label=label,
        mime_type=mime_type,
        parent_names=parents,
        human_index=human_index,
        metadata={},
    )
    assert fragment.human_file_name() == Path(expected)


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


def test_with_source(fragment):
    new_fragment = Fragment.with_source(fragment)

    assert new_fragment != fragment
    assert new_fragment.id is not None
    assert new_fragment.id != fragment.id
    assert new_fragment.label == fragment.label
    assert new_fragment.metadata == fragment.metadata

    new_fragment = Fragment.with_source(fragment, label="new_label")
    assert new_fragment != fragment
    assert new_fragment.id is not None
    assert new_fragment.id != fragment.id
    assert new_fragment.label != fragment.label
    assert new_fragment.label == "new_label"
    assert new_fragment.metadata == fragment.metadata

    new_document = Document.with_source(fragment)

    assert isinstance(new_document, Document)
    assert new_document != fragment
    assert new_document.id is not None
    assert new_document.id != fragment.id
    assert new_document.label == fragment.label
    assert new_document.metadata == fragment.metadata


def test_with_source_with_extra_metadata(fragment):
    new_fragment = Fragment.with_source(
        fragment, update_metadata={"extra_key": "extra_value"}
    )

    assert new_fragment != fragment
    assert new_fragment.id is not None
    assert new_fragment.id != fragment.id
    assert new_fragment.label == fragment.label
    assert new_fragment.metadata == {**fragment.metadata, "extra_key": "extra_value"}

def test_with_source_relationships():
    document = Document(
        id="doc_1",
        label="doc",
    )

    fragment1 = Fragment.with_source(document, label="frag1")

    assert fragment1.relationships[FragmentRelationships.SOURCE] == document.id
    assert fragment1.relationships[FragmentRelationships.SOURCE_DOCUMENT] == document.id

    fragment2 = Fragment.with_source(fragment1, label="frag2")

    assert fragment2.relationships[FragmentRelationships.SOURCE] == fragment1.id
    assert fragment2.relationships[FragmentRelationships.SOURCE_DOCUMENT] == document.id

def test_fails_with_no_source():
    fragment = Fragment(
        id="fragment_1",
        label="md",
        relationships={
            FragmentRelationships.SOURCE_DOCUMENT: "source_document"
        }        
    )

    with pytest.raises(ValueError, match="Source relationship is mandatory in fragment: "):
        Fragment.with_source(fragment)

def test_fails_with_no_source_document():
    fragment = Fragment(
        id="fragment_1",
        label="md",
        relationships={
            FragmentRelationships.SOURCE: "source"
        }
    )

    with pytest.raises(ValueError, match="Source document relationship is mandatory in source fragment: "):
        Fragment.with_source(fragment)