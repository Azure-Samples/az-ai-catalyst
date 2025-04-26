import pytest

from az_ai.ingestion import Fragment, Document

@pytest.fixture
def fragment():
    return Fragment(
        id="fragment_1",
        label="md",
        metadata={"key": "value"},
    )

@pytest.mark.parametrize(
    "label,mime_type,parents,expected",
    [
        ("di", "application/octet-stream", ["test"], "test/di.bin"),
        ("figure", "image/png", [], "figure.png"),
        ("figure", "image/png", ["test"], "test/figure.png"),

    ],
)
def test_fragment_human_file_name_with_elements(label, mime_type, parents, expected):
    fragment = Fragment(
        id="fragment",
        label=label,
        mime_type=mime_type,
        parent_names=parents,
        metadata={},
    )
    assert fragment.human_file_name() == expected



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


def test_create_from(fragment):
    new_fragment = Fragment.create_from(fragment)

    assert new_fragment != fragment
    assert new_fragment.id is not None
    assert new_fragment.id != fragment.id
    assert new_fragment.label == fragment.label
    assert new_fragment.metadata == fragment.metadata

    new_fragment = Fragment.create_from(fragment, label="new_label")
    assert new_fragment != fragment
    assert new_fragment.id is not None
    assert new_fragment.id != fragment.id
    assert new_fragment.label != fragment.label
    assert new_fragment.label == "new_label"
    assert new_fragment.metadata == fragment.metadata

    new_document = Document.create_from(fragment)

    assert isinstance(new_document, Document)
    assert new_document != fragment
    assert new_document.id is not None
    assert new_document.id != fragment.id
    assert new_document.label == fragment.label
    assert new_document.metadata == fragment.metadata


def test_create_from_with_extra_metadata(fragment):
    new_fragment = Fragment.create_from(
        fragment,
        update_metadata={"extra_key": "extra_value"}
    )

    assert new_fragment != fragment
    assert new_fragment.id is not None
    assert new_fragment.id != fragment.id
    assert new_fragment.label == fragment.label
    assert new_fragment.metadata == {
        **fragment.metadata,
        "extra_key": "extra_value"
    }