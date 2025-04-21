import pytest

from az_ai.ingestion import Fragment

@pytest.fixture
def fragment():
    return Fragment(
        id="fragment_1",
        metadata={"key": "value"},
    )

def test_fragment_initialization(fragment):
    assert fragment.id == "fragment_1"
    assert fragment.metadata == {"key": "value"}