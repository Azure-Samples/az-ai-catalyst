```mermaid
---
title: It's a RAG Ingestion Pipeline
---
flowchart TD
    Document_@{ shape: doc, label: "Document[]" }
    DocumentIntelligenceResult_document_intelligence_result@{ shape: doc, label: "DocumentIntelligenceResult[document_intelligence_result]" }
    apply_document_intelligence@{ shape: rect, label: "apply_document_intelligence" }
    DocumentIntelligenceResult_@{ shape: doc, label: "DocumentIntelligenceResult[]" }
    Figure_figure@{ shape: doc, label: "Figure[figure]" }
    extract_figures@{ shape: rect, label: "extract_figures" }
    ImageFragment_@{ shape: doc, label: "ImageFragment[]" }
    FigureDescription_figure_description@{ shape: doc, label: "FigureDescription[figure_description]" }
    describe_figure@{ shape: rect, label: "describe_figure" }
    MarkdownFragment_md_fragment@{ shape: doc, label: "MarkdownFragment[md_fragment]" }
    split_markdown@{ shape: rect, label: "split_markdown" }
    Fragment_md_fragment@{ shape: doc, label: "Fragment[md_fragment]" }
    Fragment_figure_description@{ shape: doc, label: "Fragment[figure_description]" }
    Chunk_chunk@{ shape: doc, label: "Chunk[chunk]" }
    embed@{ shape: rect, label: "embed" }
    Document_ --> apply_document_intelligence
    apply_document_intelligence --> DocumentIntelligenceResult_document_intelligence_result
    DocumentIntelligenceResult_ --> extract_figures
    extract_figures -- \* --> Figure_figure
    ImageFragment_ --> describe_figure
    describe_figure --> FigureDescription_figure_description
    DocumentIntelligenceResult_ --> split_markdown
    split_markdown -- \* --> MarkdownFragment_md_fragment
    Fragment_md_fragment -- \* --> embed
    Fragment_figure_description -- \* --> embed
    embed -- \* --> Chunk_chunk
```