from PIL import Image
from io import BytesIO

import base64

def image_binary(image: Image, mime_type):
    buffer = BytesIO()

    if mime_type == "image/jpeg":
        format = "JPEG"
    elif mime_type == "image/png":
        format = "PNG"
    else:
        raise ValueError(f"Unsupported mime type: {mime_type}")
    image.save(buffer, format=format)

    return buffer.getvalue()

def image_base64(image: Image, mime_type):
    return base64.b64encode(image_binary(image, mime_type)).decode("utf-8")

def image_data_url(image: Image, mime_type):
    return f"data:{mime_type};base64,{image_base64(image, mime_type)}"