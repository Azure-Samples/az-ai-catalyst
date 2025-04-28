import re
import base64
import logging
from io import BytesIO

import pymupdf
from azure.ai.documentintelligence.models import (
    AnalyzeResult,
    DocumentFigure,
)
from PIL import Image

from az_ai.ingestion import Fragment

logger = logging.getLogger(__name__)

def extract_code_block(markdown: str) -> str:
    compiled_pattern = re.compile(r"```(?:\w+\s+)?(.*?)```", re.DOTALL)
    matches = compiled_pattern.findall(markdown)
    return [code.strip() for code in matches]    

class MarkdownFigureExtractor:
    def extract(self, fragment: Fragment) -> list[Fragment]:
        logger.info("Extracting figures from fragment: %s", fragment)

        self.document_intelligence_result = AnalyzeResult(fragment.metadata["document_intelligence_result"])
        self.content : str = fragment.content.decode("utf-8")

        results = [
            self._extract_figure_from_page(fragment, figure_index, figure)
            for figure_index, figure in enumerate(
                self.document_intelligence_result.figures
            )
        ]
        return results

    def _extract_figure_from_page(
        self,
        fragment: Fragment,
        figure_index: int,
        figure: DocumentFigure,
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
        logger.debug(
            "Extracting figure index %d and id '%s'...", figure_index, figure.id
        )
        bounding_box, figure_content, figure_caption, page_number = (
            self._extract_bounding_box(fragment, figure)
        )
        logger.debug(
            "Figure extraction from page %s, content: %s, caption: %s, ",
            page_number,
            figure_content,
            figure_caption,
        )

        image = self._crop_image_from_page_image(
            fragment,
            page_number - 1,  # Document Intelligence page numbers are 1-based
            bounding_box,
        )
        image_data_url = self._image_data_url(image, "image/png")
        update_metadata = {
                "page_number": page_number,
                "figure_index": figure_index,
                "figure_id": figure.id,
                "data_url": image_data_url,
                "document_intelligence_result": None,
            }
        if figure_caption:
            update_metadata["caption"] = figure_caption

        return Fragment.create_from(
            fragment,
            label="figure",
            human_index=figure_index,
            content=self._image_binary(image, "image/png"),
            mime_type="image/png",
            update_metadata=update_metadata,
        )

    def _extract_bounding_box(
        self, fragment: Fragment, figure: DocumentFigure
    ):
        figure_caption = None
        figure_content = ""
        for i, span in enumerate(figure.spans):
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

    def _crop_image_from_page_image(
        self, fragment: Fragment, page_number, bounding_box
    ) -> Image:
        mime_type = fragment.metadata.get("file_type")
        if not mime_type:
            raise ValueError(
                "Unable to get mime type from Document metadata['file_type']"
            )

        if mime_type == "application/pdf":
            return self._crop_image_from_pdf_page(
                fragment.metadata["file_path"], page_number, bounding_box
            )
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

    def _image_binary(self, image: Image, mime_type):
        buffer = BytesIO()

        if mime_type == "image/jpeg":
            format = "JPEG"
        elif mime_type == "image/png":
            format = "PNG"
        else:
            raise ValueError(f"Unsupported mime type: {mime_type}")
        image.save(buffer, format=format)

        return buffer.getvalue()

    def _image_base64(self, image: Image, mime_type):
        return base64.b64encode(self._image_binary(image, mime_type)).decode("utf-8")

    def _image_data_url(self, image: Image, mime_type):
        return f"data:{mime_type};base64,{self._image_base64(image, mime_type)}"
