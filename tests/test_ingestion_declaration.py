import pytest

from typing import Annotated

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment
from az_ai.ingestion.ingestion import OperationError

@pytest.fixture
def ingestion():
    return az_ai.ingestion.Ingestion()


def test_ingestion_initialization(ingestion):
    @ingestion.operation()
    def operation(document: Document) -> Fragment:
        pass

    ingestion()


def test_ingestion_sequence_run(ingestion):
    apply_document_intelligence_has_run = 0
    extract_page_images_has_run = 0
    index = 1

    @ingestion.operation()
    def operation1(document: Document) -> Fragment:
        nonlocal apply_document_intelligence_has_run, index
        apply_document_intelligence_has_run = index
        index = index + 1

    @ingestion.operation()
    def operation2(fragment: Fragment) -> Fragment:
        nonlocal extract_page_images_has_run, index
        extract_page_images_has_run = index
        index = index + 1
    ingestion()

    assert 1 == apply_document_intelligence_has_run
    assert 2 == extract_page_images_has_run


def test_operation_should_have_return_type(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(document: Document):
            pass

def test_operation_should_have_parammeters(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation() -> Document:
            pass


def test_operation_accepts_only_fragment_parameters(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(fragment: str) -> Fragment:
            pass

def test_operation_accepts_only_fragment_return_types(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(fragment: Fragment) -> str:
            pass


def test_operation_accepts_only_dict_filter(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(fragment: Annotated[Fragment, 'text': 'wrong']) -> Fragment:
            pass

def test_operation_accepts_only_dict_metadata(ingestion):
    with pytest.raises(OperationError):
        @ingestion.operation()
        def operation(fragment: Fragment) -> Annotated[Fragment, 'text': 'wrong']:
            pass
