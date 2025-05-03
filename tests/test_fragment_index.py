import pytest

from az_ai.ingestion.repository import (
    FragmentIndex,
    FragmentIndexEntry,
)
from az_ai.ingestion.schema import (
    FragmentSelector,
    ImageFragment,
)


class TestFragment(ImageFragment):
    pass


@pytest.fixture
def index():
    return FragmentIndex(
        fragments=[
            FragmentIndexEntry(
                ref="1",
                label="fragment_label",
                types={"Fragment"},
            ),
            FragmentIndexEntry(
                ref="image_1",
                label="image_label",
                types={"ImageFragment", "Fragment"},
            ),
            FragmentIndexEntry(
                ref="document_1",
                label="document_label",
                types={"Document", "Fragment"},
            ),
            FragmentIndexEntry(
                ref="chunk_1",
                label="chunk_label",
                types={"Chunk", "Fragment"},
            ),
            FragmentIndexEntry(
                ref="test_1",
                label="test_label",
                types={"TestFragment", "ImageFragment", "Fragment"},
            ),
        ]
    )


def test_index(index):
    # Test the FragmentIndex class
    assert isinstance(index, FragmentIndex)
    assert len(index.fragments) == 5


def test_match_all(index):
    refs = index.match()

    assert len(refs) == 5


def test_match_fragment_type(index):
    refs = index.match(FragmentSelector(fragment_type="Fragment"))

    assert len(refs) == 5


def test_match_image_fragment_type(index):
    refs = index.match(FragmentSelector(fragment_type="ImageFragment"))

    assert len(refs) == 2
    assert refs[0] == "image_1"
    assert refs[1] == "test_1"


def test_match_document_type(index):
    refs = index.match(FragmentSelector(fragment_type="Document"))

    assert len(refs) == 1
    assert refs[0] == "document_1"


def test_match_chunk_type(index):
    refs = index.match(FragmentSelector(fragment_type="Chunk"))

    assert len(refs) == 1
    assert refs[0] == "chunk_1"

def test_add_entry(index):
    new_fragment = TestFragment(
        label="test_label_2",
    )
    result = index.add(new_fragment)
    entry = index.fragments[-1] 

    assert len(index.fragments) == 6
    assert result == index
    assert entry.ref == new_fragment.id
    assert entry.label == new_fragment.label
    assert entry.types == {"TestFragment", "ImageFragment", "Fragment"} 

def test_update_entry(index):
    new_fragment = TestFragment(
        id="test_1",
        label="test_label_2",
    )
    result = index.update(new_fragment)
    entry = next((e for e in index.fragments if e.ref == new_fragment.id), None)

    assert len(index.fragments) == 5
    assert result == index
    assert entry.ref == "test_1"
    assert entry.label == "test_label_2"
    assert entry.types == {"TestFragment", "ImageFragment", "Fragment"} 