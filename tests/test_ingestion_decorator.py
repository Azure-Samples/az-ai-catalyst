from typing import Annotated

import pytest

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment


@pytest.fixture
def ingestion():
    return az_ai.ingestion.Ingestion()








    return ingestion

def test_document_operation(ingestion):
    @ingestion.operation()
    def document_op(
        document: Annotated[Document, {"label": "image"}],
    ) -> Annotated[Document, "text"]:
        pass

    assert len(ingestion.operations()) == 1
    op = ingestion.operations()["document_op"]

    assert op.name == "document_op"

    assert op.input_specs[0].name == "document"
    assert op.input_specs[0].fragment_type == "Document"
    assert not op.input_specs[0].multiple
    assert op.input_specs[0].filter == {"label": "image"}

    assert op.output_spec.fragment_type == "Document"
    assert op.output_spec.multiple is False
    assert op.output_spec.label == "text"


def test_fragment_operation(ingestion):
    @ingestion.operation()
    def fragment_op(
        fragment: Annotated[Fragment, {"label": "image"}],
    ) -> Annotated[Fragment, "text"]:
        pass

    assert len(ingestion.operations()) == 1
    op = ingestion.operations()["fragment_op"]

    assert op.name == "fragment_op"

    assert op.input_specs[0].name == "fragment"
    assert op.input_specs[0].fragment_type == "Fragment"
    assert not op.input_specs[0].multiple
    assert op.input_specs[0].filter == {"label": "image"}

    assert op.output_spec.fragment_type == "Fragment"
    assert op.output_spec.multiple is False
    assert op.output_spec.label == "text"


def test_multi_operation(ingestion):
    @ingestion.operation()
    def multi_fragment_op(
        fragment: Annotated[Fragment, {"label": "image"}],
    ) -> Annotated[list[Fragment], "text"]:
        pass

    assert len(ingestion.operations()) == 1
    op = ingestion.operations()["multi_fragment_op"]

    assert op.name == "multi_fragment_op"
    assert op.input_specs[0].name == "fragment"
    assert op.input_specs[0].fragment_type == "Fragment"
    assert not op.input_specs[0].multiple
    assert op.input_specs[0].filter == {"label": "image"}

    assert op.output_spec.multiple
    assert op.output_spec.fragment_type == "Fragment"
    assert op.output_spec.label == "text"


def test_aggregation_operation(ingestion):
    @ingestion.operation()
    def aggregation_op(
        fragment: Annotated[list[Fragment], {"label": "image"}],
    ) -> Annotated[Fragment, "text"]:
        pass

    assert len(ingestion.operations()) == 1
    op = ingestion.operations()["aggregation_op"]

    assert op.name == "aggregation_op"
    assert op.input_specs[0].name == "fragment"
    assert op.input_specs[0].fragment_type == "Fragment"
    assert op.input_specs[0].multiple
    assert op.input_specs[0].filter == {"label": "image"}

    assert not op.output_spec.multiple
    assert op.output_spec.fragment_type == "Fragment"
    assert op.output_spec.label == "text"


def test_multiple_input_specs(ingestion):
    @ingestion.operation()
    def op(
        document: Document,
        fragment: Fragment,
    ) -> Annotated[Fragment, "text"]:
        pass

    assert len(ingestion.operations()) == 1
    op = ingestion.operations()["op"]

    assert op.name == "op"

    assert op.input_specs[0].name == "document"
    assert op.input_specs[0].fragment_type == "Document"
    assert not op.input_specs[0].multiple
    assert op.input_specs[0].filter == {}

    assert op.input_specs[1].name == "fragment"
    assert op.input_specs[1].fragment_type == "Fragment"
    assert not op.input_specs[1].multiple
    assert op.input_specs[1].filter == {}

    assert not op.output_spec.multiple
    assert op.output_spec.fragment_type == "Fragment"
    assert op.output_spec.label == "text"