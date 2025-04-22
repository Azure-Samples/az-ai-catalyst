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
        document: Annotated[Document, {"type": "image"}],
    ) -> Annotated[Document, {"type":"text"}]:
        pass

    @ingestion.operation()
    def fragment_op(fragment: Fragment) -> Fragment:
        pass

    @ingestion.operation()
    def fragment_op_annotated(
        fragment: Annotated[Fragment, {"type": "image"}],
    ) -> Annotated[Fragment, {"type": "text"}]:
        pass

    @ingestion.operation()
    def multi_fragment_op(fragment: Fragment) -> list[Fragment]:
        pass

    @ingestion.operation()
    def multi_fragment_op_annotated(
        fragment: Annotated[Fragment, {"type": "image"}],
    ) -> Annotated[list[Fragment], {"type": "text"}]:
        pass

    return ingestion


def test_has_n_operations(ingestion):
    assert len(ingestion.operations()) == 6


def test_document_operation(ingestion):
    op = ingestion.operations()["document_op"]
    
    assert op.name == "document_op"

    assert op.input.name == "document"
    assert op.input.input_type == Document
    assert op.output.metadata == {}

    assert op.output.output_type == Document
    assert op.output.multiple is False
    assert op.output.metadata == {}

def test_document_operation_annotated(ingestion):
    op = ingestion.operations()["document_op_annotated"]
    
    assert op.name == "document_op_annotated"

    assert op.input.name == "document"
    assert op.input.input_type == Document
    assert op.input.filter == {"type": "image"}

    assert op.output.output_type == Document
    assert op.output.multiple is False
    assert op.output.metadata == {"type": "text"}

def test_fragment_operation(ingestion):
    op = ingestion.operations()["fragment_op"]
    
    assert op.name == "fragment_op"

    assert op.input.name == "fragment"
    assert op.input.input_type == Fragment
    assert op.input.filter == {}

    assert op.output.output_type == Fragment
    assert op.output.multiple is False
    assert op.output.metadata == {}


def test_fragment_operation_annotated(ingestion):
    op = ingestion.operations()["fragment_op_annotated"]
    
    assert op.name == "fragment_op_annotated"

    assert op.input.name == "fragment"
    assert op.input.input_type == Fragment
    assert op.input.filter == {"type": "image"}

    assert op.output.output_type == Fragment
    assert op.output.multiple is False
    assert op.output.metadata == {"type": "text"}


def test_multiple_operation(ingestion):
    op = ingestion.operations()["multi_fragment_op"]

    assert op.name == "multi_fragment_op"

    assert op.input.name == "fragment"
    assert op.input.input_type == Fragment
    assert op.input.filter == {}

    assert op.output.multiple
    assert op.output.output_type == list[Fragment]
    assert op.output.metadata == {}


def test_multiple_operation_annotated(ingestion):
    op = ingestion.operations()["multi_fragment_op_annotated"]

    assert op.name == "multi_fragment_op_annotated"
    assert op.input.name == "fragment"
    assert op.input.input_type == Fragment
    assert op.input.filter == {"type": "image"}

    assert op.output.multiple
    assert op.output.output_type == list[Fragment]
    assert op.output.metadata == {"type": "text"}
