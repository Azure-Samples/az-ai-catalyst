
from az_ai.catalyst.schema import Document, Fragment, FragmentSelector


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

def test_selector_matching():
    generic_document = FragmentSelector(fragment_type="Document")
    document_with_labels = FragmentSelector(fragment_type="Document", labels=["label1", "label2"])
    document_with_label = FragmentSelector(fragment_type="Document", labels=["label1"])
    generic_fragment = FragmentSelector(fragment_type="Fragment")
    fragment_with_labels = FragmentSelector(fragment_type="Fragment", labels=["label1", "label2"])
    fragment_with_label = FragmentSelector(fragment_type="Fragment", labels=["label1"])
    
    assert generic_fragment.matches(generic_fragment)
    assert generic_fragment.matches(fragment_with_label)
    assert generic_fragment.matches(fragment_with_labels)
    assert generic_fragment.matches(generic_document)
    assert generic_fragment.matches(document_with_label)
    assert generic_fragment.matches(document_with_labels)

    assert not fragment_with_label.matches(generic_fragment)
    assert fragment_with_label.matches(fragment_with_label)
    assert not fragment_with_label.matches(fragment_with_labels)
    assert not fragment_with_label.matches(generic_document)
    assert fragment_with_label.matches(document_with_label)
    assert not fragment_with_label.matches(document_with_labels)    

    assert not fragment_with_labels.matches(generic_fragment)
    assert fragment_with_labels.matches(fragment_with_label)
    assert fragment_with_labels.matches(fragment_with_labels)
    assert not fragment_with_labels.matches(generic_document)
    assert fragment_with_labels.matches(document_with_label)
    assert fragment_with_labels.matches(document_with_labels)    

    assert not generic_document.matches(generic_fragment)
    assert not generic_document.matches(fragment_with_label)
    assert not generic_document.matches(fragment_with_labels)
    assert generic_document.matches(generic_document)
    assert generic_document.matches(document_with_label)
    assert generic_document.matches(document_with_labels)

    assert not document_with_label.matches(generic_fragment)
    assert not document_with_label.matches(fragment_with_label)
    assert not document_with_label.matches(fragment_with_labels)
    assert not document_with_label.matches(generic_document)
    assert document_with_label.matches(document_with_label)
    assert not document_with_label.matches(document_with_labels)

    assert not document_with_labels.matches(generic_fragment)
    assert not document_with_labels.matches(fragment_with_label)
    assert not document_with_labels.matches(fragment_with_labels)
    assert not document_with_labels.matches(generic_document)
    assert document_with_labels.matches(document_with_label)
    assert document_with_labels.matches(document_with_labels)



