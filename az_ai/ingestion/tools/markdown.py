import base64
import logging
from io import BytesIO
from typing import Any, List, Optional, Sequence
from pathlib import Path

import pymupdf
from azure.ai.documentintelligence.models import (
    DocumentFigure,
    AnalyzeResult,
)

from PIL import Image


from az_ai.ingestion import Fragment

logger = logging.getLogger(__name__)


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

        Path("/tmp/toto.md").write_text(self.content, encoding="utf-8")

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
        3. Generates a data URL and a textual description for the image.
        4. Formats a Markdown-compatible image description with an optional <figcaption>.
        5. Updates the document's text by injecting the Markdown image description.
        6. Constructs and returns an ImageNode containing the image data, URL, description, and related metadata.

        Args:
            document (DocumentIntelligenceDocument): The document model containing pages and figures.
            figure_index (int): Zero-based index of the figure within the document.
            figure (DocumentFigure): Metadata object representing the figure to extract.

        Returns:
            ImageNode: A node encapsulating:
                - text (str): Markdown-formatted figure description.
                - image (str): Base64-encoded image data.
                - image_url (str): The image represented as a data URL.

        Side effects:
            Updates `document.text` by injecting a figure reference in the Markdown in place of the original figure.
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
        #image_description = self._describe_image(image_data_url, figure_caption)
        #logger.debug("Image description: %s", image_description)

        # TODO: handle image description properly
        image_description="TODO: figure description"

        if figure_caption and figure_caption.strip():
            markdown_image_description = (
                f"<figcaption>{figure_caption}</figcaption>\n{image_description}"
            )
        else:
            markdown_image_description = image_description

        return Fragment.create_from(
            fragment,
            label="figure",
            human_index=figure_index,
            content=self._image_binary(image, "image/png"),
            mime_type="image/png",
            extra_metadata={
                "page_number": page_number,
                "figure_index": figure_index,
                "figure_id": figure.id,
                "data_url": image_data_url,
            },
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

    # TODO: do we still need this?
    def _generate_page_images(self, file) -> List[BytesIO]:
        pages = []
        with pymupdf.open(file) as doc:
            for page_num, page in enumerate(doc):
                logger.info("Converting page %d of %s to image...", page_num + 1, file)
                page_num += 1
                pix = page.get_pixmap()
                image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                buffer = BytesIO()
                image.save(buffer, format="png")
                buffer.seek(0)
                pages.append(buffer)
        return pages

    def _update_figure_description(self, md_content, img_description, idx):
        logger.debug(f"Updating figure description {idx}...")
        start_substring = "<figure>"
        end_substring = "</figure>"
        new_string = f"\n![](figures/{idx})\n{img_description}\n"
        new_md_content = md_content
        start_index = 0
        for i in range(idx + 1):
            start_index = md_content.find(start_substring, start_index)
            if start_index == -1:
                break
            else:
                start_index += len(start_substring)
        if start_index != -1:
            end_index = md_content.find(end_substring, start_index)
            if end_index != -1:
                new_md_content = (
                    md_content[:start_index] + new_string + md_content[end_index:]
                )
        return new_md_content
