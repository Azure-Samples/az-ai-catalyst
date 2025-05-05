from pathlib import Path
from typing import Annotated

import pytest

from az_ai.ingestion import Document, Fragment, Ingestion, OperationError
from az_ai.ingestion.repository import LocalRepository
from az_ai.ingestion.schema import FragmentSelector
from az_ai.ingestion.settings import IngestionSettings


@pytest.fixture(scope="function")
def ingestion(tmpdir):
    return Ingestion(settings=IngestionSettings(repository_path=Path(tmpdir)))

@pytest.fixture(scope="function")
def single_step_ingestion(ingestion):
    @ingestion.operation()
    def simple(input: Document) -> Annotated[Fragment, "output_label"]:
        return Fragment(
            id="output_id",
            label="output_label",
            metadata=input.metadata | { "extra_key": "extra_value" },
        )
    return ingestion

@pytest.fixture(scope="function")
def two_step_ingestion(ingestion):
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



def test_single_ingestion(single_step_ingestion, document):
    assert len(single_step_ingestion.repository.find()) == 0
    single_step_ingestion.repository.store(document)

    single_step_ingestion()

    fragment = single_step_ingestion.repository.get("output_id")
    assert fragment.id == "output_id"
    assert fragment.label == "output_label"
    assert fragment.metadata == document.metadata | { "extra_key": "extra_value" }

    assert len(single_step_ingestion.repository.find()) == 2

def test_double_ingestion(two_step_ingestion, document):
    assert len(two_step_ingestion.repository.find()) == 0
    two_step_ingestion.repository.store(document)

    two_step_ingestion()

    fragment = two_step_ingestion.repository.get("output_id")
    assert fragment.id == "output_id"
    assert fragment.label == "output_label"
    assert fragment.metadata == document.metadata | { "extra_key": "extra_value" }

    fragment = two_step_ingestion.repository.get("second_id")
    assert fragment.id == "second_id"
    assert fragment.label == "second_output_label"
    assert fragment.metadata == fragment.metadata | { "extra_key2": "extra_value2" }

    assert len(two_step_ingestion.repository.find()) == 3


def test_return_is_compliant_with_signature(ingestion, document):
    ingestion.repository.store(document)
    
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


def test_processing_scope(ingestion):
    doc1 = ingestion.repository.store(Document(label="doc1", parent_names=["doc1"]))
    doc2 = ingestion.repository.store(Document(label="doc2", parent_names=["doc2"]))

    class Single(Fragment):
        pass

    class Multi(Fragment):
        pass

    doc1_single = ingestion.repository.store(Single.with_source(doc1, label="doc1_single"))
    doc1_multi1 = ingestion.repository.store(Multi.with_source(doc1, label="doc1_multi1"))
    doc1_multi2 = ingestion.repository.store(Multi.with_source(doc1, label="doc1_multi2"))

    doc2_single = ingestion.repository.store(Single.with_source(doc2, label="doc2_single"))
    doc2_multi1 = ingestion.repository.store(Multi.with_source(doc2, label="doc2_multi1"))
    doc2_multi2 = ingestion.repository.store(Multi.with_source(doc2, label="doc2_multi2"))

    @ingestion.operation()
    def op_same(
        single: Single,
        multi: list[Multi],
    ) -> Annotated[Fragment, "output_same"]:
        return Fragment.with_source(single, label="output_same", update_metadata={
            "multi_size": len(multi),
            "single": single.id,
            "multi1": multi[0].id,
            "multi2": multi[1].id,
        })

    @ingestion.operation(scope="all")
    def op_all(
        single: Single,
        multi: list[Multi],
    ) -> Annotated[Fragment, "output_all"]:
        return Fragment.with_source(single, label="output_all", update_metadata={
            "multi_size": len(multi),
            "single": single.id,
            "multi": set((multi[0].id, multi[1].id, multi[2].id, multi[3].id)),
        })
    
    ingestion()

    outputs = ingestion.repository.find(FragmentSelector(fragment_type="Fragment", labels=["output_same"]))

    assert len(outputs) == 2
    output1 = next(output for output in outputs if output.source_document_ref() == doc1.id)
    output2 = next(output for output in outputs if output.source_document_ref() == doc2.id)
    assert output1.metadata["multi_size"] == 2
    assert output1.metadata["single"] == doc1_single.id
    assert output1.metadata["multi1"] == doc1_multi1.id
    assert output1.metadata["multi2"] == doc1_multi2.id

    assert output2.metadata["multi_size"] == 2
    assert output2.metadata["single"] == doc2_single.id
    assert output2.metadata["multi1"] == doc2_multi1.id
    assert output2.metadata["multi2"] == doc2_multi2.id

    outputs = ingestion.repository.find(FragmentSelector(fragment_type="Fragment", labels=["output_all"]))

    assert len(outputs) == 2
    output1 = next(output for output in outputs if output.source_document_ref() == doc1.id)
    output2 = next(output for output in outputs if output.source_document_ref() == doc2.id)
    assert output1.metadata["multi_size"] == 4
    assert output1.metadata["single"] == doc1_single.id
    assert set(output1.metadata["multi"]) == set((doc1_multi1.id, doc1_multi2.id, doc2_multi1.id, doc2_multi2.id))

    assert output2.metadata["multi_size"] == 4
    assert output2.metadata["single"] == doc2_single.id
    assert set(output2.metadata["multi"]) == set((doc1_multi1.id, doc1_multi2.id, doc2_multi1.id, doc2_multi2.id))
