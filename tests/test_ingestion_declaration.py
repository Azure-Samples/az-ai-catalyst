from typing import Annotated

import pytest

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment
from az_ai.ingestion.ingestion import OperationError
from az_ai.ingestion.settings import IngestionSettings


@pytest.fixture
def ingestion(tmpdir):
    return az_ai.ingestion.Ingestion(settings=IngestionSettings(repository_path=str(tmpdir)))


def test_ingestion_initialization(ingestion):
    ingestion.operation()

    def operation(document: Document) -> Annotated[Fragment, "text"]:
        pass


def test_operation_should_have_return_type(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(document: Document):
            pass


def test_operation_should_have_annotated_return_type(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(document: Document) -> Fragment:
            pass


def test_operation_should_have_parammeters(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation() -> Annotated[Document, "text"]:
            pass


def test_operation_accepts_only_fragment_parameters(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(fragment: str) -> Annotated[Fragment, "text"]:
            pass


def test_operation_accepts_only_fragment_return_types(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(fragment: Fragment) -> Annotated[str, "text"]:
            pass


def test_operation_accepts_only_dict_filter(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(fragment: Annotated[Fragment, "text":"wrong"]) -> Annotated[Fragment, "text"]:
            pass


def test_operation_accepts_only_dict_metadata(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(fragment: Fragment) -> Annotated[Fragment, "text":"wrong"]:
            pass
