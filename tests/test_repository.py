from pathlib import Path

import pytest

from az_ai.ingestion import Fragment, FragmentSpec, Document
from az_ai.ingestion.repository import (
    FragmentNotFoundError,
    LocalRepository,
    DuplicateFragmentError,
    FragmentContentNotFoundError,
)
from az_ai.ingestion.schema import OperationsLogEntry, FragmentSpec


@pytest.fixture
def fragment():
    return Fragment(
        id="fragment_id",
        label="fragment_label",
        metadata={"key": "fragment_value"},
    )


@pytest.fixture
def document():
    return Document(
        id="doc_id",
        label="doc_label",
        metadata={"key": "document_value"},
        content_url="file:README.md",
    )


@pytest.fixture(scope="function")
def empty_repository(tmpdir, fragment, document):
    return LocalRepository(path=Path(tmpdir))


@pytest.fixture(scope="function")
def repository(tmpdir, fragment, document):
    repository = LocalRepository(path=Path(tmpdir))

    repository.store(fragment)
    repository.store(document)
    return repository


def test_store(empty_repository, fragment):
    empty_repository.store(fragment)
    retrieved_fragment = empty_repository.get(fragment.id)

    assert retrieved_fragment == fragment


def test_update(repository, fragment):
    fragment.label = "updated_label"
    repository.update(fragment)

    retrieved_fragment = repository.get(fragment.id)
    assert retrieved_fragment.label == "updated_label"


def test_find_fragment(repository, fragment):
    spec = FragmentSpec(fragment_type="Fragment", label="fragment_label")
    retrieved_fragments = repository.find(spec)

    assert len(retrieved_fragments) == 1
    assert retrieved_fragments[0] == fragment


def test_find_document(repository, document):
    retrieved_fragments = repository.find()
    assert len(retrieved_fragments) == 2

    spec = FragmentSpec(fragment_type="Document", label="doc_label")
    retrieved_fragments = repository.find(spec)

    assert len(retrieved_fragments) == 1
    assert retrieved_fragments[0] == document

    spec = FragmentSpec(fragment_type="Document", label="wrong_doc_label")
    retrieved_fragments = repository.find(spec)

    assert len(retrieved_fragments) == 0


def test_find_all(repository, fragment, document):
    retrieved_fragments = repository.find()

    assert len(retrieved_fragments) == 2
    assert all(f in retrieved_fragments for f in [fragment, document])


def test_fragment_not_found(repository):
    with pytest.raises(FragmentNotFoundError):
        repository.get("non_existent_id")


def test_no_path_provided():
    with pytest.raises(ValueError):
        LocalRepository()


def test_duplicate_insert(empty_repository, fragment):
    empty_repository.store(fragment)
    with pytest.raises(DuplicateFragmentError):
        empty_repository.store(fragment)


def test_document_store(empty_repository, document):
    empty_repository.store(document)

    retrieved_document = empty_repository.get(document.id)

    assert retrieved_document == document


def test_fragment_content_not_found(empty_repository, fragment):
    pytest.skip(
        "Skipping test for FragmentContentNotFoundError until content_ref is implemented"
    )
    empty_repository.store(fragment)

    with pytest.raises(FragmentContentNotFoundError):
        empty_repository.get_content(fragment.id)


def test_document_content_from_content_url(empty_repository, document):
    empty_repository.store(document)

    content = empty_repository.get_content(document.id)

    assert open("README.md", "rb").read() == content


def test_add_operations_log_entry(empty_repository):
    entry1 = OperationsLogEntry(
        operation_name="operation_name1", input_refs=["foo"], output_refs=["bar", "baz"]
    )
    entry2 = OperationsLogEntry(
        operation_name="operation_name2",
        input_refs=["bar"],
        output_refs=["barbar", "barbaz"],
    )
    entry3 = OperationsLogEntry(
        operation_name="operation_name2",
        input_refs=["baz"],
        output_refs=["bazbar", "bazbaz"],
    )
    empty_repository.add_operations_log_entry(entry1)
    empty_repository.add_operations_log_entry(entry2)
    empty_repository.add_operations_log_entry(entry3)

    entries = empty_repository.find_operations_log_entry()
    assert entries == [entry1, entry2, entry3]

    entries = empty_repository.find_operations_log_entry(
        operation_name="operation_name1"
    )
    assert entries == [entry1]

    entries = empty_repository.find_operations_log_entry(
        operation_name="operation_name2"
    )
    assert entries == [entry2, entry3]

    entries = empty_repository.find_operations_log_entry(
        operation_name="operation_name2", input_fragment_ref="bar"
    )
    assert entries == [entry2]

    entries = empty_repository.find_operations_log_entry(
        operation_name="operation_name2", input_fragment_ref="baz"
    )
    assert entries == [entry3]

    entries = empty_repository.find_operations_log_entry(operation_name="does_not_exist")
    assert entries == []
