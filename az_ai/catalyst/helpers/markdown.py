import logging
import re

import pymupdf
from azure.ai.documentintelligence.models import (
    DocumentFigure,
)
from PIL import Image

from az_ai.catalyst import DocumentIntelligenceResult, Fragment, ImageFragment

logger = logging.getLogger(__name__)


def extract_code_block(markdown: str) -> str:
    compiled_pattern = re.compile(r"```(?:\w+\s+)?(.*?)```", re.DOTALL)
    matches = compiled_pattern.findall(markdown)
    return [code.strip() for code in matches]


class MarkdownFigureExtractor:
    def extract(self, di_fragment: DocumentIntelligenceResult, figure_class: type = ImageFragment, min_dimension: int = 0.5) -> list[Fragment]:
        logger.info("Extracting figures from fragment: %s", di_fragment)

        self.document_intelligence_result = di_fragment.analyze_result()
        self.content: str = di_fragment.content_as_str()
        self._min_dimension = min_dimension

        if not self.document_intelligence_result.figures:
            logger.info("No figures found in the document.")
            return []

        results = [
            frag
            for i, fig in enumerate(self.document_intelligence_result.figures)
            if (frag := self._extract_figure_from_page(di_fragment, i, fig, figure_class)) is not None
        ]
        return results

    def _extract_figure_from_page(
        self,
        fragment: DocumentIntelligenceResult,
        figure_index: int,
        figure: DocumentFigure,
        figure_class: type,
    ):
        """
        Extracts a figure from a page in the document.

        This method performs the following steps:
        1. Retrieves the bounding box, content text, caption, and page number for the given figure.
        2. Crops the corresponding page image to produce the figure image.
        3. Generates a data URL
        4. Return an image Fragment containing the image data URL related metadata.

        Args:
            document (DocumentIntelligenceDocument): The document model containing pages and figures.
            figure_index (int): Zero-based index of the figure within the document.
            figure (DocumentFigure): Metadata object representing the figure to extract.

        Returns:
            Fragment: A Fragment object containing the extracted figure image and metadata.
        """
        logger.debug("Extracting figure index %d and id '%s'...", figure_index, figure.id)
        bounding_box, figure_content, figure_caption, page_number = self._extract_bounding_box(fragment, figure)
        # Ensure bounding box is large enough to process (expected format: x0, y0, x1, y1)
        x0, y0, x1, y1 = bounding_box
        width = abs(x1 - x0)
        height = abs(y1 - y0)

        logger.debug(
            "Figure extraction from page %s, content: %s, caption: %s, ",
            page_number,
            figure_content,
            figure_caption,
        )

        if width < self._min_dimension and height < self._min_dimension:
            logger.debug(
                "Skipping figure id '%s' on page %s: bounding box too small (%.1f x %.1f < %dx%d)",
                getattr(figure, "id", None),
                page_number,
                width,
                height,
                self._min_dimension,
                self._min_dimension,
            )
            return None

        image = self._crop_image_from_page_image(
            fragment,
            page_number - 1,  # Document Intelligence page numbers are 1-based
            bounding_box,
        )
        update_metadata = {
            "page_number": page_number,
            "figure_index": figure_index,
            "figure_id": figure.id,
            "document_intelligence_result": None,
        }
        if figure_caption:
            update_metadata["caption"] = figure_caption

        return figure_class.with_source(
            fragment,
            label="figure",
            human_index=figure_index,
            update_metadata=update_metadata,
        ).set_content_from_image(image, "image/png")

    def _extract_bounding_box(self, fragment: Fragment, figure: DocumentFigure):
        figure_caption = None
        figure_content = ""
        for span in figure.spans:  # TODO: ???
            figure_content += self.content[span.offset : span.offset + span.length]
        if figure.caption:
            figure_caption = figure.caption.content
            caption_region = figure.caption.bounding_regions
            for region in figure.bounding_regions:
                if region not in caption_region:
                    logger.info("Figure with caption found")
                    boundingbox = (
                        region.polygon[0],  # x0 (left)
                        region.polygon[1],  # y0 (top)
                        region.polygon[4],  # x1 (right)
                        region.polygon[5],  # y1 (bottom)
                    )
        else:
            logger.debug("Figure witout caption")
            for region in figure.bounding_regions:
                boundingbox = (
                    region.polygon[0],  # x0 (left)
                    region.polygon[1],  # y0 (upper)
                    region.polygon[4],  # x1 (right)
                    region.polygon[5],  # y1 (lower)
                )
        return boundingbox, figure_content, figure_caption, region.page_number

    def _crop_image_from_page_image(self, fragment: Fragment, page_number, bounding_box) -> Image:
        mime_type = fragment.metadata.get("file_type")
        if not mime_type:
            raise ValueError("Unable to get mime type from Document metadata['file_type']")

        if mime_type == "application/pdf":
            return self._crop_image_from_pdf_page(fragment.metadata["file_path"], page_number, bounding_box)
        else:
            return self._crop_image_from_image(page_number, bounding_box)

    def _crop_image_from_pdf_page(self, file_path, page_number, bounding_box):
        with pymupdf.open(file_path) as doc:
            page = doc.load_page(page_number)

            # Cropping the page. The rect requires the coordinates in the format (x0, y0, x1, y1).
            bbx = [x * 72 for x in bounding_box]
            rect = pymupdf.Rect(bbx)
            pix = page.get_pixmap(matrix=pymupdf.Matrix(300 / 72, 300 / 72), clip=rect)

            return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    def _crop_image_from_image(self, page_number, bounding_box):
        with Image.open(self.page_images[page_number]) as img:
            if img.format == "TIFF":
                img.seek(page_number)
                img = img.copy()

            # The bounding box is expected to be in the format (left, upper, right, lower).
            cropped_image = img.crop(bounding_box)
            return cropped_image
