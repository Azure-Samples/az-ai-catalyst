import pytest

import az_ai.ingestion
from az_ai.ingestion import Document, Fragment
from az_ai.ingestion.ingestion import TransformationError

@pytest.fixture
def ingestion():
    return az_ai.ingestion.Ingestion()


def test_ingestion_initialization(ingestion):
    @ingestion.transformation()
    def apply_document_intelligence(document: Document) -> Fragment:
        pass

    ingestion()


def test_ingestion_sequence_run(ingestion):
    apply_document_intelligence_has_run = 0
    extract_page_images_has_run = 0
    index = 1

    @ingestion.transformation()
    def apply_document_intelligence(document: Document) -> Fragment:
        nonlocal apply_document_intelligence_has_run, index
        apply_document_intelligence_has_run = index
        index = index + 1

    @ingestion.transformation()
    def extract_page_images(fragment: Fragment) -> Fragment:
        nonlocal extract_page_images_has_run, index
        extract_page_images_has_run = index
        index = index + 1
    ingestion()

    assert 1 == apply_document_intelligence_has_run
    assert 2 == extract_page_images_has_run


def test_transformation_should_have_return_type(ingestion):
    with pytest.raises(TransformationError):
        @ingestion.transformation()
        def apply_document_intelligence(document: Document):
            pass

def test_transformation_should_have_parammeters(ingestion):
    with pytest.raises(TransformationError):
        @ingestion.transformation()
        def apply_document_intelligence() -> Document:
            pass
