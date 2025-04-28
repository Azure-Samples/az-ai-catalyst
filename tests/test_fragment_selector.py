import pytest

from az_ai.ingestion.schema import FragmentSelector, Fragment, Document

def test_fragment_selector():
    selector = FragmentSelector(fragment_type="Fragment", labels=["label1", "label2"])
    assert selector.fragment_type == "Fragment"
    assert selector.labels == ["label1", "label2"]


def test_fragment_selector_match_type():
    selector = FragmentSelector(fragment_type="Fragment")

    assert selector.matches(Fragment(label="label1"))
    assert selector.matches(Fragment(label="label2"))
    assert selector.matches(Document(label="label1"))

def test_fragment_selector_match_document_type():
    selector = FragmentSelector(fragment_type="Document")

    assert selector.matches(Document(label="label1"))
    assert selector.matches(Document(label="label2"))
    assert not selector.matches(Fragment(label="label1"))


def test_fragment_selector_match_single_label():
    selector = FragmentSelector(fragment_type="Document", labels=["label1"])

    assert selector.matches(Document(label="label1"))
    assert not selector.matches(Document(label="label2"))
    assert not selector.matches(Document(label="label3"))
    assert not selector.matches(Fragment(label="label1"))

def test_fragment_selector_match_multiple_labels():
    selector = FragmentSelector(fragment_type="Document", labels=["label1", "label2"])

    assert selector.matches(Document(label="label1"))
    assert not selector.matches(Fragment(label="label2"))
    assert not selector.matches(Fragment(label="label3"))
    assert not selector.matches(Fragment(label="label1"))
    assert not selector.matches(Fragment(label="label2"))

