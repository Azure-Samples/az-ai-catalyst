import json

from urllib import request
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient

from az_ai.catalyst.repository import (
    DuplicateFragmentError,
    FragmentContentNotFoundError,
    FragmentIndex,
    FragmentNotFoundError,
    Repository,
)
from az_ai.catalyst.schema import Fragment, FragmentSelector, OperationsLog, OperationsLogEntry

CONTENT_PREFIX = "_content"
FRAGMENTS_PREFIX = "_fragments"

class AzureRepository(Repository):
    def __init__(self, connection_string: str, container_name: str):
        """
        Initialize the AzureRepository with a connection string and container name.
        """
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        if not self.container_client.exists():
            self.container_client.create_container()
        
        # Create virtual "folders" to match LocalRepository structure
        self._contents_prefix = f"{CONTENT_PREFIX}/"
        self._fragments_prefix = f"{FRAGMENTS_PREFIX}/"
        self._operations_log_path = "_operations_log.json"
        self._index_path = f"{self._fragments_prefix}_index.json"
        
        # Initialize operations log and index if they don't exist
        if not self._blob_exists(self._operations_log_path):
            self._write_log(OperationsLog())
        if not self._blob_exists(self._index_path):
            self._write_index(FragmentIndex())

    def get(self, reference: str) -> Fragment:
        """Get the fragment for the given reference."""
        fragment_path = self._fragment_path(reference)
        try:
            blob_client = self.container_client.get_blob_client(fragment_path)
            fragment_data = blob_client.download_blob().readall().decode('utf-8')
            fragment = Fragment.from_json(fragment_data)
            if fragment.content_ref:
                fragment.content = self._get_content_from_ref(fragment)
            return fragment
        except ResourceNotFoundError as exc:
            raise FragmentNotFoundError(f"Fragment {reference} not found in Azure Blob Storage.") from exc

    def store(self, fragment: Fragment) -> Fragment:
        """Store the given fragment."""
        fragment_path = self._fragment_path(fragment)
        
        if self._blob_exists(fragment_path):
            raise DuplicateFragmentError(f"Fragment {fragment.id} already exists in Azure Blob Storage.")
        
        # Handle content from URL if available
        if not fragment.content and "content_url" in fragment.__class__.model_fields and fragment.content_url:
            fragment.content = self._get_content_from_url(fragment)
        
        # Store content if available
        if fragment.content:
            self._store_content(fragment)
        
        # Store fragment
        blob_client = self.container_client.get_blob_client(fragment_path)
        blob_client.upload_blob(fragment.model_dump_json(indent=2))
        
        # Update index
        self._write_index(self._read_index().add(fragment))
        
        return fragment

    def update(self, fragment: Fragment) -> Fragment:
        """Update the given fragment."""
        fragment_path = self._fragment_path(fragment)
        
        if not self._blob_exists(fragment_path):
            raise FragmentNotFoundError(f"Fragment {fragment.id} does not exist in Azure Blob Storage.")
        
        # Update content if provided
        if fragment.content:
            self._store_content(fragment, update_link=False)
        
        # Update fragment
        blob_client = self.container_client.get_blob_client(fragment_path)
        blob_client.upload_blob(fragment.model_dump_json(indent=2), overwrite=True)
        
        # Update index
        self._write_index(self._read_index().update(fragment))
        
        return fragment

    def find(self, selector: FragmentSelector = None, with_content: bool = True) -> list[Fragment]:
        """Get all fragments matching the given spec."""
        fragments = []
        index = self._read_index()
        
        for fragment_ref in index.match(selector):
            fragment_path = self._fragment_path(fragment_ref)
            try:
                blob_client = self.container_client.get_blob_client(fragment_path)
                fragment_data = blob_client.download_blob().readall().decode('utf-8')
                fragment = Fragment.from_json(fragment_data)
                fragments.append(self.get(fragment.id) if with_content else fragment)
            except ResourceNotFoundError:
                # Skip fragments that might be in the index but not in storage (inconsistent state)
                pass
        
        return fragments

    def add_operations_log_entry(self, operations_log_entry: OperationsLogEntry) -> None:
        """Add an operation log entry to the repository."""
        log = self._read_log()
        log.entries.append(operations_log_entry)
        self._write_log(log)


    def find_operations_log_entry(self, operation_name: str = None, input_fragment_refs: set[str] = None):
        """
        Find an operation log entry by operation_name and/or input_fragment_ref
        """
        return [
            entry
            for entry in self._read_log().entries
            if (not operation_name or operation_name == entry.operation_name)
            and (
                not input_fragment_refs
                or (
                    len(input_fragment_refs) == len(entry.input_refs)
                    and all(ref in entry.input_refs for ref in input_fragment_refs)
                )
            )
        ]

    def _read_log(self) -> OperationsLog:
        """Read the operations log from blob storage."""
        try:
            blob_client = self.container_client.get_blob_client(self._operations_log_path)
            log_data = blob_client.download_blob().readall().decode('utf-8')
            return OperationsLog.model_validate_json(log_data)
        except ResourceNotFoundError:
            return OperationsLog()

    def _write_log(self, log: OperationsLog):
        """Write the operations log to blob storage."""
        blob_client = self.container_client.get_blob_client(self._operations_log_path)
        blob_client.upload_blob(log.model_dump_json(indent=2), overwrite=True)

    def _read_index(self) -> FragmentIndex:
        """Read the fragment index from blob storage."""
        try:
            blob_client = self.container_client.get_blob_client(self._index_path)
            index_data = blob_client.download_blob().readall().decode('utf-8')
            return FragmentIndex.model_validate_json(index_data)
        except ResourceNotFoundError:
            return FragmentIndex()

    def _write_index(self, index: FragmentIndex):
        """Write the fragment index to blob storage."""
        blob_client = self.container_client.get_blob_client(self._index_path)
        blob_client.upload_blob(index.model_dump_json(indent=2), overwrite=True)

    def _store_content(self, fragment: Fragment, update_link: bool = True) -> None:
        """Store the content of the fragment in blob storage."""
        if fragment.content_ref is None:
            fragment.content_ref = fragment.id
        
        content_path = self._content_path(fragment)
        blob_client = self.container_client.get_blob_client(content_path)
        blob_client.upload_blob(fragment.content, overwrite=True)

    def _get_content_from_ref(self, fragment: Fragment) -> bytes:
        """Get content for a fragment from its content_ref."""
        content_path = self._content_path(fragment)
        try:
            blob_client = self.container_client.get_blob_client(content_path)
            return blob_client.download_blob().readall()
        except ResourceNotFoundError:
            return None

    def _fragment_path(self, fragment_or_ref: str | Fragment) -> str:
        """Get the blob path for a fragment."""
        if isinstance(fragment_or_ref, Fragment):
            return f"{self._fragments_prefix}{fragment_or_ref.__class__.class_name()}/{fragment_or_ref.id}.json"
        else:
            # Need to find the right path by listing fragments
            prefix = f"{self._fragments_prefix}"
            blobs = list(self.container_client.list_blobs(name_starts_with=prefix))
            
            for blob in blobs:
                # Looking for blobs ending with /{fragment_or_ref}.json
                if blob.name.endswith(f"/{fragment_or_ref}.json"):
                    return blob.name
            
            raise FragmentNotFoundError(f"Fragment {fragment_or_ref} not found in Azure Blob Storage.")

    def _content_path(self, fragment: Fragment) -> str:
        """Get the blob path for fragment content."""
        if not fragment.content_ref:
            raise FragmentContentNotFoundError(f"Fragment {fragment.id} does not have a content reference.")
        return f"{self._contents_prefix}{fragment.content_ref}"
    
    def _blob_exists(self, blob_path: str) -> bool:
        """Check if a blob exists."""
        blob_client = self.container_client.get_blob_client(blob_path)
        try:
            blob_client.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False
