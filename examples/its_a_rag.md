```mermaid
---
title: It's a RAG Ingestion Pipeline
---
flowchart TD
    Fragment_figure_description@{ shape: doc, label: "Fragment[figure_description]" }
    Embedding@{ shape: doc, label: "Embedding" }
    Fragment_md@{ shape: doc, label: "Fragment[md]" }
    ImageFragment_figure@{ shape: doc, label: "ImageFragment[figure]" }
    Fragment_di_result@{ shape: doc, label: "Fragment[di_result]" }
    apply_document_intelligence@{ shape: rect, label: "apply_document_intelligence" }
    apply_document_intelligence --> Fragment_di_result
    Document --> apply_document_intelligence
    extract_figures@{ shape: rect, label: "extract_figures" }
    extract_figures --> ImageFragment_figure
    Fragment_di_result --> extract_figures
    describe_figure@{ shape: rect, label: "describe_figure" }
    describe_figure --> Fragment_figure_description
    ImageFragment_figure --> describe_figure
    split_markdown@{ shape: rect, label: "split_markdown" }
    split_markdown --> Fragment_md
    Fragment_di_result --> split_markdown
    embed@{ shape: rect, label: "embed" }
    embed --> Embedding
    Fragment_md --> embed
    Fragment_figure_description --> embed

```