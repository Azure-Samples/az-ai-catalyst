from abc import ABC, abstractmethod
from pathlib import Path
from urllib import request

from az_ai.ingestion.schema import (
    Fragment,
    FragmentSelector,
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
    def find(self, selector: FragmentSelector = None, with_content: bool = True) -> list[Fragment]:
        """
        Get all fragments matching the given spec.
        """
        pass

    @abstractmethod
    def add_operations_log_entry(self, operations_log_entry: OperationsLogEntry) -> None:
        """
        Add an operation log entry to the repository.
        """
        pass

    @abstractmethod
    def human_content_path(self, fragment: Fragment) -> Path:
        """
        Get the human-readable path for the given fragment content.
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


CONTENT_PREFIX = "_content"
FRAGMENTS_PREFIX = "_fragments"
HUMAN_PREFIX = "_human"

class LocalRepository(Repository):
    def __init__(self, path: Path = None):
        """
        Initialize the LocalRepository at the given path.
        """
        if path is None:
            raise ValueError("Path must be provided.")
        self._path = path
        self._contents_path = path / CONTENT_PREFIX
        self._fragments_path = path / FRAGMENTS_PREFIX
        self._human_path = path / HUMAN_PREFIX
        self._operations_log_path = path / "_operations_log.json"
        self._contents_path.mkdir(parents=True, exist_ok=True)
        self._fragments_path.mkdir(parents=True, exist_ok=True)
        self._human_path.mkdir(parents=True, exist_ok=True)
        if not self._operations_log_path.exists():
            self._operations_log_path.write_text(OperationsLog().model_dump_json(indent=2))

    def human_path(self) -> Path:
        return self._human_path

    def get(self, reference: str) -> Fragment:
        """Get the value for the given key."""
        fragment_path = self._fragment_path(reference)
        if not fragment_path.exists():
            raise FragmentNotFoundError(f"Fragment {reference} not found.")

        with open(fragment_path) as f:
            fragment = Fragment.from_json(f.read())
            if fragment.content_ref:
                fragment.content = self._get_content_from_ref(fragment)
            elif "content_url" in fragment.__class__.model_fields and fragment.content_url:
                fragment.content = self._get_content_from_url(fragment)
                self.update(fragment)
        return fragment

    def store(self, fragment: Fragment) -> Fragment:
        """Store the given fragment."""

        fragment_path = self._fragment_path(fragment)
        if fragment_path.exists():
            raise DuplicateFragmentError(f"Fragment {fragment.id} already exists.")
        if fragment.content:
            self._store_content(fragment)
        fragment_path.write_text(fragment.model_dump_json(indent=2))
        self._create_human_fragment_link(fragment, fragment_path)

        return fragment

    def update(self, fragment: Fragment) -> Fragment:
        """Update the given fragment."""

        fragment_path = self._fragment_path(fragment)
        if not fragment_path.exists():
            raise FragmentNotFoundError(f"Fragment {fragment.id} does not exist.")
        if fragment.content:
            self._store_content(fragment)
        fragment_path.write_text(fragment.model_dump_json(indent=2))

        return fragment

    def find(self, selector: FragmentSelector = None, with_content: bool = True) -> list[Fragment]:
        """
        Get all fragments matching the given spec.
        """
        # TODO: this is a very naive search implementation
        # and should be improved for performance.
        # For now, we just read through all fragments and filter them.
        fragments = []
        for fragment_path in self._fragments_path.glob("*.json"):
            fragment = Fragment.from_json(fragment_path.read_text())
            if selector is None or selector.matches(fragment):
                fragments.append(self.get(fragment.id) if with_content else fragment)

        return fragments

    def get_human_path(self, fragment: Fragment) -> Path:
        """
        Get the human-readable path for the given fragment.
        """
        return self._human_path / fragment.human_file_name()

    def add_operations_log_entry(self, operations_log_entry: OperationsLogEntry) -> None:
        """
        Add an operation log entry to the repository.
        """
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
        return OperationsLog.model_validate_json(self._operations_log_path.read_bytes())

    def _write_log(self, log: OperationsLog):
        self._operations_log_path.write_text(log.model_dump_json(indent=2))

    def _store_content(self, fragment: Fragment) -> None:
        """
        Store the content of the fragment.
        """

        if fragment.content_ref is None:
            fragment.content_ref = fragment.id
        content_path = self._content_path(fragment)
        content_path.write_bytes(fragment.content)
        self._create_human_content_link(fragment, content_path)

    def human_content_path(self, fragment: Fragment) -> Path:
        """
        Get the human-readable path for the given fragment content.
        """
        return self._human_path / CONTENT_PREFIX / fragment.human_file_name()

    def _create_human_content_link(self, fragment: Fragment, content_path: Path):
        """
        Create a human-readable link for the given fragment content.
        """
        human_path = self.human_content_path(fragment)
        if human_path.exists():
            raise DuplicateFragmentError(f"Fragment {fragment.id} human content name already exists.")
        if not human_path.parent.exists():
            human_path.parent.mkdir(parents=True, exist_ok=True)
        human_path.symlink_to(
            Path("/".join([".." for i in range(len(fragment.human_file_name().parents) + 1)]))
            / CONTENT_PREFIX
            / content_path.name
        )

    def _create_human_fragment_link(self, fragment: Fragment, fragment_path: Path):
        """
        Create a human-readable link for the given fragment.
        """
        human_path = self._human_path / FRAGMENTS_PREFIX / fragment.human_file_name()
        human_path = human_path.with_suffix(".json")
        if human_path.exists():
            raise DuplicateFragmentError(f"Fragment {fragment.id} human fragment name already exists.")
        if not human_path.parent.exists():
            human_path.parent.mkdir(parents=True, exist_ok=True)
        human_path.symlink_to(
            Path("/".join([".." for i in range(len(fragment.human_file_name().parents) + 1)]))
            / FRAGMENTS_PREFIX
            / fragment_path.name
        )

    def _get_content_from_url(self, fragment: Fragment) -> bytes:
        """
        Get the content from the given URL.
        """
        if not fragment.content_url:
            raise FragmentContentNotFoundError(f"Fragment {fragment.id} does not have a content URL.")
        try:
            with request.urlopen(fragment.content_url) as response:
                return response.read()
        except Exception as exc:
            raise FragmentContentNotFoundError(f"Failed to fetch content from {fragment.content_url}: {e}") from exc

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
        reference = fragment_or_ref.id if isinstance(fragment_or_ref, Fragment) else fragment_or_ref
        return self._path / self._fragments_path / f"{reference}.json"

    def _content_path(self, fragment: FragmentNotFoundError) -> Path:
        """
        Get the path to the content file.
        """
        if not fragment.content_ref:
            raise FragmentContentNotFoundError(f"Fragment {fragment.id} does not have a content reference.")
        return self._contents_path / fragment.content_ref
