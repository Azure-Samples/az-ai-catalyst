```mermaid
---
title: It's a RAG Ingestion Pipeline
---
flowchart TD
    Fragment{_document_intelligence_result}@{ shape: doc, label: "Fragment[document_intelligence_result]" }
    Fragment{_md_fragment}@{ shape: doc, label: "Fragment[md_fragment]" }
    Chunk{_chunk}@{ shape: doc, label: "Chunk[chunk]" }
    Fragment{_figure}@{ shape: doc, label: "Fragment[figure]" }
    Fragment{_figure_description}@{ shape: doc, label: "Fragment[figure_description]" }
    apply_document_intelligence@{ shape: rect, label: "apply_document_intelligence" }
    apply_document_intelligence --> Fragment{_document_intelligence_result}
    Document{ --> apply_document_intelligence
    extract_figures@{ shape: rect, label: "extract_figures" }
    extract_figures --> Fragment{_figure}
    Fragment{_document_intelligence_result} --> extract_figures
    describe_figure@{ shape: rect, label: "describe_figure" }
    describe_figure --> Fragment{_figure_description}
    Fragment{_figure} --> describe_figure
    split_markdown@{ shape: rect, label: "split_markdown" }
    split_markdown --> Fragment{_md_fragment}
    Fragment{_document_intelligence_result} --> split_markdown
    embed@{ shape: rect, label: "embed" }
    embed --> Chunk{_chunk}
    Fragment{_md_fragment,figure_description} --> embed

```