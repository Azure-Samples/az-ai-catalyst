```mermaid
---
title: Argus Ingestion Pipeline
---
flowchart TD
    Document_@{ shape: doc, label: "Document[]" }
    DocumentIntelligenceResult_document_intelligence_result@{ shape: doc, label: "DocumentIntelligenceResult[document_intelligence_result]" }
    apply_document_intelligence@{ shape: rect, label: "apply_document_intelligence" }
    ImageFragment_page_image@{ shape: doc, label: "ImageFragment[page_image]" }
    split_to_page_images@{ shape: rect, label: "split_to_page_images" }
    Fragment_document_intelligence_result@{ shape: doc, label: "Fragment[document_intelligence_result]" }
    Fragment_page_image@{ shape: doc, label: "Fragment[page_image]" }
    Extraction_llm_result@{ shape: doc, label: "Extraction[llm_result]" }
    apply_llm_to_pages@{ shape: rect, label: "apply_llm_to_pages" }
    DocumentIntelligenceResult_@{ shape: doc, label: "DocumentIntelligenceResult[]" }
    Summary_summary@{ shape: doc, label: "Summary[summary]" }
    extract_summary@{ shape: rect, label: "extract_summary" }
    Fragment_llm_result@{ shape: doc, label: "Fragment[llm_result]" }
    ExtractionEvaluation_evaluated_result@{ shape: doc, label: "ExtractionEvaluation[evaluated_result]" }
    evaluate_with_llm@{ shape: rect, label: "evaluate_with_llm" }
    Document_ --> apply_document_intelligence
    apply_document_intelligence --> DocumentIntelligenceResult_document_intelligence_result
    Document_ --> split_to_page_images
    split_to_page_images -- \* --> ImageFragment_page_image
    Fragment_document_intelligence_result -- \* --> apply_llm_to_pages
    Fragment_page_image -- \* --> apply_llm_to_pages
    apply_llm_to_pages --> Extraction_llm_result
    DocumentIntelligenceResult_ --> extract_summary
    extract_summary --> Summary_summary
    Fragment_llm_result -- \* --> evaluate_with_llm
    Fragment_page_image -- \* --> evaluate_with_llm
    evaluate_with_llm --> ExtractionEvaluation_evaluated_result
```