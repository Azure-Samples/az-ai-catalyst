from typing import Annotated

import pytest

import az_ai.catalyst
from az_ai.catalyst import Document, Fragment
from az_ai.catalyst.catalyst import OperationError
from az_ai.catalyst.settings import CatalystSettings


@pytest.fixture
def catalyst(tmpdir):
    return az_ai.catalyst.Catalyst(settings=CatalystSettings(repository_path=str(tmpdir)))


def test_catalyst_initialization(catalyst):
    catalyst.operation()

    def operation(document: Document) -> Annotated[Fragment, "text"]:
        pass


def test_operation_should_have_return_type(catalyst):
    with pytest.raises(OperationError):
        @catalyst.operation()
        def operation(document: Document):
            pass


def test_operation_should_have_annotated_return_type(catalyst):
    with pytest.raises(OperationError):
        @catalyst.operation()
        def operation(document: Document) -> Fragment:
            pass


def test_operation_should_have_parammeters(catalyst):
    with pytest.raises(OperationError):
        @catalyst.operation()
        def operation() -> Annotated[Document, "text"]:
            pass


def test_operation_accepts_only_fragment_parameters(catalyst):
    with pytest.raises(OperationError):
        @catalyst.operation()
        def operation(fragment: str) -> Annotated[Fragment, "text"]:
            pass


def test_operation_accepts_only_fragment_return_types(catalyst):
    with pytest.raises(OperationError):
        @catalyst.operation()
        def operation(fragment: Fragment) -> Annotated[str, "text"]:
            pass


def test_operation_accepts_only_dict_filter(catalyst):
    with pytest.raises(OperationError):
        @catalyst.operation()
        def operation(fragment: Annotated[Fragment, "text":"wrong"]) -> Annotated[Fragment, "text"]:
            pass


def test_operation_accepts_only_dict_metadata(catalyst):
    with pytest.raises(OperationError):
        @catalyst.operation()
        def operation(fragment: Fragment) -> Annotated[Fragment, "text":"wrong"]:
            pass
