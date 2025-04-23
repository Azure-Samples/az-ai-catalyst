from abc import ABC, abstractmethod
from pathlib import Path

from az_ai.ingestion.schema import Fragment, FragmentSpec


class Repository(ABC):
    @abstractmethod
    def get(self, reference: str) -> str:
        """Get the value for the given key."""
        pass

    @abstractmethod
    def store(self, fragment: Fragment) -> Fragment:
        """Store the given fragment."""
        pass


class FragmentNotFoundError(Exception):
    """Exception raised when a fragment is not found in the repository."""

    pass


class DuplicateFragmentError(Exception):
    """Exception raised when a fragment with the same ID already exists in the repository."""

    pass


class LocalRepository(Repository):
    def __init__(self, path: Path = None):
        """
        Initialize the LocalRepository at the given path.
        """
        if path is None:
            raise ValueError("Path must be provided.")
        self._path = path
        self._path.mkdir(parents=True, exist_ok=True)

    def get(self, reference: str) -> str:
        """Get the value for the given key."""
        fragment_path = self._path / f"{reference}.json"
        if not fragment_path.exists():
            raise FragmentNotFoundError(f"Fragment {reference} not found.")

        with open(fragment_path, "r") as f:
            return Fragment.parse_raw(f.read())

    def store(self, fragment: Fragment) -> Fragment:
        """Store the given fragment."""

        fragment_path = self._path / f"{fragment.id}.json"
        if fragment_path.exists():
            raise DuplicateFragmentError(f"Fragment {fragment.id} already exists.")
        with open(fragment_path, "w") as f:
            f.write(fragment.json())

        return fragment

    def find(self, spec: FragmentSpec = None) -> list[Fragment]:
        """
        Get all fragments matching the given spec.
        """
        fragments = []
        for fragment_path in self._path.glob("*.json"):
            with open(fragment_path, "r") as f:
                fragment = Fragment.from_json(f.read())
                if spec is None or spec.matches(fragment):
                    fragments.append(fragment)

        return fragments
