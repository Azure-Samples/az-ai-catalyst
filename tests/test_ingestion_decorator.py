from typing import Annotated

import pytest

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment


@pytest.fixture
def ingestion():
    ingestion = az_ai.ingestion.Ingestion()

    @ingestion.operation()
    def document_op(
        document: Annotated[Document, {"label": "image"}],
    ) -> Annotated[Document, "text"]:
        pass

    @ingestion.operation()
    def fragment_op(
        fragment: Annotated[Fragment, {"label": "image"}],
    ) -> Annotated[Fragment, "text"]:
        pass

    @ingestion.operation()
    def multi_fragment_op(
        fragment: Annotated[Fragment, {"label": "image"}],
    ) -> Annotated[list[Fragment], "text"]:
        pass

    @ingestion.operation()
    def aggregation_op(
        fragment: Annotated[list[Fragment], {"label": "image"}],
    ) -> Annotated[Fragment, "text"]:
        pass

    return ingestion


def test_has_n_operations(ingestion):
    assert len(ingestion.operations()) == 4



def test_document_operation(ingestion):
    op = ingestion.operations()["document_op"]

    assert op.name == "document_op"

    assert op.input.name == "document"
    assert op.input.fragment_type == "Document"
    assert not op.input.multiple
    assert op.input.filter == {"label": "image"}

    assert op.output.fragment_type == "Document"
    assert op.output.multiple is False
    assert op.output.label == "text"


def test_fragment_operation(ingestion):
    op = ingestion.operations()["fragment_op"]

    assert op.name == "fragment_op"

    assert op.input.name == "fragment"
    assert op.input.fragment_type == "Fragment"
    assert not op.input.multiple
    assert op.input.filter == {"label": "image"}

    assert op.output.fragment_type == "Fragment"
    assert op.output.multiple is False
    assert op.output.label == "text"


def test_multiple_operation(ingestion):
    op = ingestion.operations()["multi_fragment_op"]

    assert op.name == "multi_fragment_op"
    assert op.input.name == "fragment"
    assert op.input.fragment_type == "Fragment"
    assert not op.input.multiple
    assert op.input.filter == {"label": "image"}

    assert op.output.multiple
    assert op.output.fragment_type == "Fragment"
    assert op.output.label == "text"


def test_aggregation_operation(ingestion):
    op = ingestion.operations()["aggregation_op"]

    assert op.name == "aggregation_op"
    assert op.input.name == "fragment"
    assert op.input.fragment_type == "Fragment"
    assert op.input.multiple
    assert op.input.filter == {"label": "image"}

    assert not op.output.multiple
    assert op.output.fragment_type == "Fragment"
    assert op.output.label == "text"