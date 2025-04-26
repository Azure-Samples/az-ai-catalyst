from abc import ABC, abstractmethod
from pathlib import Path
from urllib import request
from az_ai.ingestion.schema import (
    Fragment,
    FragmentSpec,
    OperationsLog,
    OperationsLogEntry,
)


class Repository(ABC):
    @abstractmethod
    def get(self, reference: str) -> str:
        """Get the value for the given key."""
        pass

    @abstractmethod
    def store(self, fragment: Fragment) -> Fragment:
        """Store the given fragment."""
        pass

    @abstractmethod
    def update(self, fragment: Fragment) -> Fragment:
        """Update the given fragment."""
        pass

    @abstractmethod
    def find(self, spec: FragmentSpec = None) -> list[Fragment]:
        """
        Get all fragments matching the given spec.
        """
        pass

    @abstractmethod
    def add_operations_log_entry(
        self, operations_log_entry: OperationsLogEntry
    ) -> None:
        """
        Add an operation log entry to the repository.
        """
        pass


class FragmentNotFoundError(Exception):
    """Exception raised when a fragment is not found in the repository."""

    pass


class DuplicateFragmentError(Exception):
    """Exception raised when a fragment with the same ID already exists in the repository."""

    pass


class FragmentContentNotFoundError(Exception):
    """Exception raised when a fragment does not have a content URL."""

    pass


class LocalRepository(Repository):
    def __init__(self, path: Path = None):
        """
        Initialize the LocalRepository at the given path.
        """
        if path is None:
            raise ValueError("Path must be provided.")
        self._path = path
        self._contents_path = path / "_content"
        self._fragments_path = path / "_fragments"
        self._human_path = path / "_human"
        self._operations_log_path = path / "_operations_log.json"
        self._contents_path.mkdir(parents=True, exist_ok=True)
        self._fragments_path.mkdir(parents=True, exist_ok=True)
        self._human_path.mkdir(parents=True, exist_ok=True)
        if not self._operations_log_path.exists():
            self._operations_log_path.write_text(
                OperationsLog().model_dump_json(indent=2)
            )

    def get(self, reference: str) -> Fragment:
        """Get the value for the given key."""
        fragment_path = self._fragment_path(reference)
        if not fragment_path.exists():
            raise FragmentNotFoundError(f"Fragment {reference} not found.")

        with open(fragment_path, "r") as f:
            return Fragment.from_json(f.read())

    def store(self, fragment: Fragment) -> Fragment:
        """Store the given fragment."""

        fragment_path = self._fragment_path(fragment)
        if fragment_path.exists():
            raise DuplicateFragmentError(f"Fragment {fragment.id} already exists.")
        fragment_path.write_text(fragment.model_dump_json(indent=2))

        return fragment

    def update(self, fragment: Fragment) -> Fragment:
        """Update the given fragment."""

        fragment_path = self._fragment_path(fragment)
        if not fragment_path.exists():
            raise FragmentNotFoundError(f"Fragment {fragment.id} does not exist.")
        fragment_path.write_text(fragment.model_dump_json(indent=2))

        return fragment

    def find(self, spec: FragmentSpec = None) -> list[Fragment]:
        """
        Get all fragments matching the given spec.
        """
        # TODO: this is a very naive search implementation
        # and should be improved for performance.
        # For now, we just read through all fragments and filter them.
        fragments = []
        for fragment_path in self._fragments_path.glob("*.json"):
            with open(fragment_path, "r") as f:
                fragment = Fragment.from_json(f.read())
                if spec is None or spec.matches(fragment):
                    fragments.append(fragment)

        return fragments

    def add_operations_log_entry(
        self, operations_log_entry: OperationsLogEntry
    ) -> None:
        """
        Add an operation log entry to the repository.
        """
        log = self._read_log()
        log.entries.append(operations_log_entry)
        self._write_log(log)

    def find_operations_log_entry(
        self, operation_name: str = None, input_fragment_ref: str | Fragment = None
    ):
        """
        Find an operation log entry by operation_name and/or input_fragment_ref
        """
        if input_fragment_ref and isinstance(input_fragment_ref, Fragment):
            input_fragment_ref = input_fragment_ref.id
        return [
            entry
            for entry in self._read_log().entries
            if (not operation_name or operation_name == entry.operation_name)
            and (
                not input_fragment_ref
                or input_fragment_ref in entry.input_refs
            )
        ]

    def _read_log(self) -> OperationsLog:
        return OperationsLog.parse_file(self._operations_log_path)

    def _write_log(self, log: OperationsLog):
        self._operations_log_path.write_text(log.model_dump_json(indent=2))

    def get_content(self, reference: str) -> bytes:
        """Get the content of the fragment."""
        fragment = self.get(reference)

        if not fragment.content_url:
            raise FragmentContentNotFoundError(
                f"Fragment {reference} does not have a content URL."
            )

        if fragment.content_ref:
            return self._get_content_from_ref(fragment)

        content = self._get_content_from_url(fragment)
        self._store_content(fragment, content)
        self.update(fragment)

        return content

    def _store_content(self, fragment: Fragment, content: bytes) -> None:
        """
        Store the content of the fragment.
        """

        if fragment.content_ref is None:
            fragment.content_ref = fragment.id
        content_path = self._content_path(fragment)
        content_path.write_bytes(content)
        self._create_human_content_link(fragment, content_path)

    def _create_human_content_link(self, fragment: Fragment, content_path: Path):
        """
        Create a human-readable link for the given fragment.
        """
        human_path = self._human_path / fragment.human_name()
        if human_path.exists():
            raise DuplicateFragmentError(
                f"Fragment {fragment.id} human content name already exists."
            )
        if not human_path.parent.exists():
            human_path.parent.mkdir(parents=True, exist_ok=True)
        human_path.symlink_to(content_path)

    def _get_content_from_url(self, fragment: Fragment) -> bytes:
        """
        Get the content from the given URL.
        """
        content_url = fragment.content_url
        try:
            with request.urlopen(content_url) as response:
                return response.read()
        except Exception as e:
            raise FragmentContentNotFoundError(
                f"Failed to fetch content from {content_url}: {e}"
            )

    def _get_content_from_ref(self, fragment: Fragment) -> bytes:
        content_path = self._content_path(fragment)

        if not content_path.exists():
            return None
        else:
            with open(content_path, "rb") as f:
                return f.read()

    def _fragment_path(self, fragment_or_ref: str | Fragment) -> Path:
        """
        Get the path to the fragment file.
        """
        if isinstance(fragment_or_ref, Fragment):
            reference = fragment_or_ref.id
        else:
            reference = fragment_or_ref
        return self._path / self._fragments_path / f"{reference}.json"

    def _content_path(self, fragment: FragmentNotFoundError) -> Path:
        """
        Get the path to the content file.
        """
        if not fragment.content_ref:
            raise FragmentContentNotFoundError(
                f"Fragment {fragment.id} does not have a content reference."
            )
        return self._contents_path / fragment.content_ref
