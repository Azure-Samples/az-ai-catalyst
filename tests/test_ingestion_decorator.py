from typing import Annotated

import pytest

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment
from az_ai.ingestion.ingestion import OperationError


@pytest.fixture
def ingestion():
    ingestion = az_ai.ingestion.Ingestion()

    @ingestion.operation()
    def document_op(document: Document) -> Document:
        pass

    @ingestion.operation()
    def document_op_annotated(
        document: Annotated[Document, {'label': 'image'}],
    ) -> Annotated[Document, 'text']:
        pass

    @ingestion.operation()
    def fragment_op(fragment: Fragment) -> Fragment:
        pass

    @ingestion.operation()
    def fragment_op_annotated(
        fragment: Annotated[Fragment, {'label': 'image'}],
    ) -> Annotated[Fragment, 'text']:
        pass

    @ingestion.operation()
    def multi_fragment_op(fragment: Fragment) -> list[Fragment]:
        pass

    @ingestion.operation()
    def multi_fragment_op_annotated(
        fragment: Annotated[Fragment, {'label': 'image'}],
    ) -> Annotated[list[Fragment], 'text']:
        pass

    return ingestion


def test_has_n_operations(ingestion):
    assert len(ingestion.operations()) == 6


def test_document_operation(ingestion):
    op = ingestion.operations()["document_op"]
    
    assert op.name == "document_op"

    assert op.input.name == "document"
    assert op.input.fragment_type == Document
    assert op.input.filter == {}

    assert op.output.fragment_type == Document
    assert op.output.multiple is False
    assert op.output.label is None

def test_document_operation_annotated(ingestion):
    op = ingestion.operations()["document_op_annotated"]
    
    assert op.name == "document_op_annotated"

    assert op.input.name == "document"
    assert op.input.fragment_type == Document
    assert op.input.filter == {"label": "image"}

    assert op.output.fragment_type == Document
    assert op.output.multiple is False
    assert op.output.label == "text"
    
def test_fragment_operation(ingestion):
    op = ingestion.operations()["fragment_op"]
    
    assert op.name == "fragment_op"

    assert op.input.name == "fragment"
    assert op.input.fragment_type == Fragment
    assert op.input.filter == {}

    assert op.output.fragment_type == Fragment
    assert op.output.multiple is False
    assert op.output.label is None


def test_fragment_operation_annotated(ingestion):
    op = ingestion.operations()["fragment_op_annotated"]
    
    assert op.name == "fragment_op_annotated"

    assert op.input.name == "fragment"
    assert op.input.fragment_type == Fragment
    assert op.input.filter == {"label": "image"}

    assert op.output.fragment_type == Fragment
    assert op.output.multiple is False
    assert op.output.label == "text"


def test_multiple_operation(ingestion):
    op = ingestion.operations()["multi_fragment_op"]

    assert op.name == "multi_fragment_op"

    assert op.input.name == "fragment"
    assert op.input.fragment_type == Fragment
    assert op.input.filter == {}

    assert op.output.multiple
    assert op.output.fragment_type == Fragment
    assert op.output.label is None


def test_multiple_operation_annotated(ingestion):
    op = ingestion.operations()["multi_fragment_op_annotated"]

    assert op.name == "multi_fragment_op_annotated"
    assert op.input.name == "fragment"
    assert op.input.fragment_type == Fragment
    assert op.input.filter == {"label": "image"}

    assert op.output.multiple
    assert op.output.fragment_type == Fragment
    assert op.output.label == "text"
