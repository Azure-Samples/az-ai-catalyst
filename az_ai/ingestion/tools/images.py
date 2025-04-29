import base64
from io import BytesIO

from PIL.Image import Image


def image_binary(image: Image, mime_type: str) -> bytes:
    buffer = BytesIO()

    if mime_type == "image/jpeg":
        format = "JPEG"
    elif mime_type == "image/png":
        format = "PNG"
    else:
        raise ValueError(f"Unsupported mime type: {mime_type}")
    image.save(buffer, format=format)

    return buffer.getvalue()

def image_base64(image: Image | bytes, mime_type: str):
    if isinstance(image, Image):
        image = image_binary(image, mime_type)
        return base64.b64encode(image).decode("utf-8")
    else:
        return base64.b64encode(image).decode("utf-8")

def image_data_url(image: Image | bytes, mime_type):
    return f"data:{mime_type};base64,{image_base64(image, mime_type)}"