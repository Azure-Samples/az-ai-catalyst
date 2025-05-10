import os
import uuid

import pytest
from azure.storage.blob import BlobServiceClient

from az_ai.catalyst import Document, Fragment, FragmentSelector
from az_ai.catalyst.azure_repository import AzureRepository
from az_ai.catalyst.repository import (
    DuplicateFragmentError,
    FragmentNotFoundError,
)
from az_ai.catalyst.schema import OperationsLogEntry


@pytest.fixture(scope="function")
def azure_repository():
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    container_name = os.environ.get("AZURE_STORAGE_CONTAINER_NAME")
    if not connection_string or not container_name:
        pytest.skip("Azure connection string or container name not set in environment variables")
    # Use a unique container for isolation if needed
    repo = AzureRepository(connection_string, container_name)
    yield repo
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs()
        for blob in blobs:
            container_client.delete_blob(blob.name)
        print(f"Cleaned up all test blobs in container {container_name}")
    except Exception as e:
        print(f"Warning: Failed to clean up Azure container: {e}")


@pytest.fixture
def fragment():
    return Fragment(
        id=f"fragment-{uuid.uuid4()}",
        label="fragment_label",
        metadata={"key": "fragment_value"},
    )


@pytest.fixture
def document():
    return Document(
        id=f"doc-{uuid.uuid4()}",
        label="doc_label",
        metadata={"key": "document_value"},
        content_url=None,
        content=b"DOCUMENT CONTENT",
    )


def test_store_and_get_fragment(azure_repository, fragment):
    azure_repository.store(fragment)
    retrieved = azure_repository.get(fragment.id)
    assert retrieved.id == fragment.id
    assert retrieved.label == fragment.label


def test_update_fragment(azure_repository, fragment):
    azure_repository.store(fragment)
    fragment.label = "updated_label"
    azure_repository.update(fragment)
    retrieved = azure_repository.get(fragment.id)
    assert retrieved.label == "updated_label"


def test_find_fragment(azure_repository, fragment):
    azure_repository.store(fragment)
    selector = FragmentSelector(fragment_type="Fragment", labels=["fragment_label"])
    results = azure_repository.find(selector)
    assert any(f.id == fragment.id for f in results)


def test_duplicate_insert(azure_repository, fragment):
    azure_repository.store(fragment)
    with pytest.raises(DuplicateFragmentError):
        azure_repository.store(fragment)


def test_fragment_not_found(azure_repository):
    with pytest.raises(FragmentNotFoundError):
        azure_repository.get("non-existent-id")


def test_document_store_and_get(azure_repository, document):
    azure_repository.store(document)
    retrieved = azure_repository.get(document.id)
    assert retrieved.id == document.id
    assert retrieved.label == document.label
    assert retrieved.content == document.content


def test_add_operations_log_entry(azure_repository):
    entry = OperationsLogEntry(operation_name="azure_op", input_refs=["foo"], output_refs=["bar"], duration_ns=123)
    azure_repository.add_operations_log_entry(entry)
    # No direct retrieval API, but ensure no exception is raised


def test_find_all(azure_repository, fragment, document):
    azure_repository.store(fragment)
    azure_repository.store(document)
    results = azure_repository.find()
    ids = [f.id for f in results]
    assert fragment.id in ids
    assert document.id in ids
