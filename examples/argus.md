```mermaid
---
title: Argus Ingestion Pipeline
---
flowchart TD
    Document@{ shape: doc, label: "Document[]" }

    apply_document_intelligence@{ shape: rect, label: "apply_document_intelligence" }
    split_to_page_images@{ shape: rect, label: "split_to_page_images" }
    extract_summary@{ shape: rect, label: "extract_summary" }
    apply_llm_to_pages@{ shape: rect, label: "apply_llm_to_pages" }
    evaluate_with_llm@{ shape: rect, label: "evaluate_with_llm" }

    DocumentIntelligenceResult_document_intelligence_result@{ shape: doc, label: "DocumentIntelligenceResult[document_intelligence_result]" }
    ImageFragment_page_image@{ shape: doc, label: "ImageFragment[page_image]" }
    Summary_summary@{ shape: doc, label: "Summary[summary]" }
    Extraction_extraction@{ shape: doc, label: "Extraction[extraction]" }
    ExtractionEvaluation_extraction_evaluation@{ shape: doc, label: "ExtractionEvaluation[extraction_evaluation]" }

    Document --> apply_document_intelligence
    apply_document_intelligence --> DocumentIntelligenceResult_document_intelligence_result

    Document --> split_to_page_images
    split_to_page_images -- \* --> ImageFragment_page_image

    DocumentIntelligenceResult_document_intelligence_result --> extract_summary
    extract_summary --> Summary_summary

    DocumentIntelligenceResult_document_intelligence_result --> apply_llm_to_pages
    ImageFragment_page_image -- \* --> apply_llm_to_pages
    apply_llm_to_pages --> Extraction_extraction

    Extraction_extraction --> evaluate_with_llm
    ImageFragment_page_image -- \* --> evaluate_with_llm
    evaluate_with_llm --> ExtractionEvaluation_extraction_evaluation

```