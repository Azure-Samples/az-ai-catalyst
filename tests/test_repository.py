from pathlib import Path

import pytest

from az_ai.ingestion import Fragment, FragmentSpec, Document
from az_ai.ingestion.repository import FragmentNotFoundError, LocalRepository


@pytest.fixture
def repository(tmpdir):
    return LocalRepository(path=Path(tmpdir))

@pytest.fixture
def fragment():
    return Fragment(
        id="test_id",
        label="test_label",
        metadata={"key": "value"},
    )

def test_store(repository, fragment):
    repository.store(fragment)
    retrieved_fragment = repository.get(fragment.id)

    assert retrieved_fragment == fragment

def test_find_fragment(repository, fragment):
    repository.store(fragment)
    
    spec = FragmentSpec(
        fragment_type=Fragment,
        label="test_label"
    )
    retrieved_fragments = repository.find(spec)

    assert len(retrieved_fragments) == 1
    assert retrieved_fragments[0] == fragment

def test_find_document(repository):
    document = Document(
        id="doc_id",
        label="doc_label",
        metadata={"key": "value"},
    )
    repository.store(document)
    
    spec = FragmentSpec(
        fragment_type=Document,
        label="doc_label"
    )
    retrieved_fragments = repository.find(spec)

    assert len(retrieved_fragments) == 1
    assert retrieved_fragments[0] == document


def test_fragment_not_found(repository):
    with pytest.raises(FragmentNotFoundError):
        repository.get("non_existent_id")

def test_no_path_provided():
    with pytest.raises(ValueError):
        LocalRepository()