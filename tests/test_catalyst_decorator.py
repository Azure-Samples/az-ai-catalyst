from typing import Annotated

import pytest

import az_ai.catalyst
from az_ai.catalyst import Document, Fragment
from az_ai.catalyst.settings import CatalystSettings


@pytest.fixture
def catalyst(tmpdir):
    return az_ai.catalyst.Catalyst(settings=CatalystSettings(repository_path=str(tmpdir)))

def test_document_operation(catalyst):
    @catalyst.operation()
    def document_op(
        document: Annotated[Document, {"label": "image"}],
    ) -> Annotated[Document, "text"]:
        pass

    assert len(catalyst.operations()) == 1
    op = catalyst.operations()["document_op"]

    assert op.name == "document_op"

    assert op.input_specs[0].name == "document"
    assert op.input_specs[0].fragment_type == "Document"
    assert not op.input_specs[0].multiple
    assert op.input_specs[0].filter == {"label": "image"}

    assert op.output_spec.fragment_type == "Document"
    assert op.output_spec.multiple is False
    assert op.output_spec.label == "text"


def test_fragment_operation(catalyst):
    @catalyst.operation()
    def fragment_op(
        fragment: Annotated[Fragment, {"label": "image"}],
    ) -> Annotated[Fragment, "text"]:
        pass

    assert len(catalyst.operations()) == 1
    op = catalyst.operations()["fragment_op"]

    assert op.name == "fragment_op"

    assert op.input_specs[0].name == "fragment"
    assert op.input_specs[0].fragment_type == "Fragment"
    assert not op.input_specs[0].multiple
    assert op.input_specs[0].filter == {"label": "image"}

    assert op.output_spec.fragment_type == "Fragment"
    assert op.output_spec.multiple is False
    assert op.output_spec.label == "text"


def test_multi_operation(catalyst):
    @catalyst.operation()
    def multi_fragment_op(
        fragment: Annotated[Fragment, {"label": "image"}],
    ) -> Annotated[list[Fragment], "text"]:
        pass

    assert len(catalyst.operations()) == 1
    op = catalyst.operations()["multi_fragment_op"]

    assert op.name == "multi_fragment_op"
    assert op.input_specs[0].name == "fragment"
    assert op.input_specs[0].fragment_type == "Fragment"
    assert not op.input_specs[0].multiple
    assert op.input_specs[0].filter == {"label": "image"}

    assert op.output_spec.multiple
    assert op.output_spec.fragment_type == "Fragment"
    assert op.output_spec.label == "text"


def test_aggregation_operation(catalyst):
    @catalyst.operation()
    def aggregation_op(
        fragment: Annotated[list[Fragment], {"label": "image"}],
    ) -> Annotated[Fragment, "text"]:
        pass

    assert len(catalyst.operations()) == 1
    op = catalyst.operations()["aggregation_op"]

    assert op.name == "aggregation_op"
    assert op.input_specs[0].name == "fragment"
    assert op.input_specs[0].fragment_type == "Fragment"
    assert op.input_specs[0].multiple
    assert op.input_specs[0].filter == {"label": "image"}

    assert not op.output_spec.multiple
    assert op.output_spec.fragment_type == "Fragment"
    assert op.output_spec.label == "text"


def test_multiple_input_specs(catalyst):
    @catalyst.operation()
    def op(
        document: Document,
        fragment: Fragment,
    ) -> Annotated[Fragment, "text"]:
        pass

    assert len(catalyst.operations()) == 1
    op = catalyst.operations()["op"]

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