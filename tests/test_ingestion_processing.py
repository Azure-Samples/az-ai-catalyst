from pathlib import Path

import pytest

from az_ai.ingestion import Ingestion,Fragment, Document
from az_ai.ingestion.repository import FragmentNotFoundError, LocalRepository


@pytest.fixture
def repository(tmpdir):
    return LocalRepository(path=Path(tmpdir))

@pytest.fixture()
def ingestion(repository):
    ingestion = Ingestion(repository=repository)

    @ingestion.operation()
    def simple(input: Document) -> Fragment:
        return Fragment(
            id="fragment_id",
            label=input.label,
            metadata=input.metadata,
        )
    return ingestion

@pytest.fixture
def document():
    return Document(
        id="document_id",
        label="document_label",
        metadata={"key": "value"},
    )


def test_ingestion(ingestion, repository, document):
    repository.store(document)

    ingestion()

    fragment = repository.get("fragment_id")
    assert fragment.id == "fragment_id"
    assert fragment.label == document.label
    assert fragment.metadata == document.metadata