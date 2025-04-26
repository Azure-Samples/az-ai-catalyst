from pathlib import Path
from typing import Annotated
import pytest

from az_ai.ingestion import Ingestion,Fragment, Document, OperationError
from az_ai.ingestion.repository import FragmentNotFoundError, LocalRepository


@pytest.fixture(scope="function")
def empty_repository(tmpdir):
    return LocalRepository(path=Path(tmpdir))

@pytest.fixture()
def single_step_ingestion(empty_repository):
    ingestion = Ingestion(repository=empty_repository)

    @ingestion.operation()
    def simple(input: Document) -> Annotated[Fragment, "document_label"]:
        return Fragment(
            id="output_id",
            label=input.label,
            metadata=input.metadata | { "extra_key": "extra_value" },
        )
    return ingestion

@pytest.fixture()
def two_step_ingestion(empty_repository):
    ingestion = Ingestion(repository=empty_repository)

    @ingestion.operation()
    def simple(input: Document) -> Annotated[Fragment, "output_label"]:
        return Fragment(
            id="output_id",
            label="output_label",
            metadata=input.metadata | { "extra_key": "extra_value" },
        )
    
    @ingestion.operation()
    def second(input: Annotated[Fragment, {'label': "output_label"}]) -> Annotated[Fragment, "second_output_label"]:
        return Fragment(
            id="second_id",
            label="second_output_label",
            metadata=input.metadata | { "extra_key2": "extra_value2" },
        )

    return ingestion

@pytest.fixture
def document():
    return Document(
        id="document_id",
        label="document_label",
        metadata={"key": "value"},
    )

@pytest.fixture
def fragment():
    return Fragment(
        id="fragment_id",
        label="fragment_label",
        metadata={"key": "fragment_value"},
    )

@pytest.fixture(scope="function")
def repository(tmpdir, document, fragment):
    repository =  LocalRepository(path=Path(tmpdir))
    repository.store(fragment)
    repository.store(document)
    return repository



def test_ingestion(single_step_ingestion, empty_repository, document):
    assert len(empty_repository.find()) == 0
    empty_repository.store(document)

    single_step_ingestion()

    fragment = empty_repository.get("output_id")
    assert fragment.id == "output_id"
    assert fragment.label == document.label
    assert fragment.metadata == document.metadata | { "extra_key": "extra_value" }

    assert len(empty_repository.find()) == 2

def test_double_ingestion(two_step_ingestion, empty_repository, document):
    assert len(empty_repository.find()) == 0
    empty_repository.store(document)

    print(two_step_ingestion.mermaid())
    two_step_ingestion()

    fragment = empty_repository.get("output_id")
    assert fragment.id == "output_id"
    assert fragment.label == "output_label"
    assert fragment.metadata == document.metadata | { "extra_key": "extra_value" }

    fragment = empty_repository.get("second_id")
    assert fragment.id == "second_id"
    assert fragment.label == "second_output_label"
    assert fragment.metadata == fragment.metadata | { "extra_key2": "extra_value2" }

    assert len(empty_repository.find()) == 3


def test_return_is_compliant_with_signature(empty_repository, document):
    ingestion = Ingestion(repository=empty_repository)
    empty_repository.store(document)
    
    @ingestion.operation()
    def simple(input: Document) -> Annotated[Fragment, "expected_label"]:
        return Fragment(
            id="output_id",
            label="wrong_label",
            metadata=input.metadata | { "extra_key": "extra_value" },
        )

    with pytest.raises(OperationError) as excinfo:
        ingestion()

    assert "Non compliant Fragment returned" in str(excinfo.value)