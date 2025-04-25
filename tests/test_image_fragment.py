import pytest

from az_ai.ingestion import ImageFragment, Fragment

METADATA = {
    "file_name": "test.png",
    "file_path": "/home/user/az-ai-ingestion/tests/data/test.png",
    "file_size": 92971,
    "mime_type": "image/png",
}


@pytest.fixture
def image():
    return ImageFragment(
        id="image_1",
        label="source",
        metadata=dict(METADATA),
    )


def test_image_human_name(image):
    assert image.human_name() == "test.png"


def test_image_initialization(image):
    assert image.id == "image_1"
    assert image.label == "source"
    assert image.metadata == METADATA


def test_document_deserialization(image):
    document_json = """{ 
        "id": "image_1",
        "label": "png",
        "type": "ImageFragment",
        "metadata": {"key": "value"}
    }"""
    deserialized_image = Fragment.from_json(document_json)
    assert deserialized_image.id == image.id
    assert deserialized_image.label == "png"
    assert deserialized_image.metadata == {"key": "value"}

