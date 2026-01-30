import os
import pymupdf
from PIL import Image

from cc_statement_extractor.shared.logger import logger

def pdf_to_images(pdf_path: str) -> list[Image.Image]:
    """
    Converts a PDF file to a list of images.
    Args:
        pdf_path (str): The path to the PDF file.
    Returns:
        list[Image.Image]: A list of images converted from the PDF.
    """
    logger.debug(f"Converting PDF file {pdf_path} to images")
    sanitized_pdf_path = os.path.abspath(pdf_path)

    if not os.path.isfile(sanitized_pdf_path):
        logger.error(f"The file {sanitized_pdf_path} does not exist.")
        raise FileNotFoundError(f"The file {sanitized_pdf_path} does not exist.")
    
    if not sanitized_pdf_path.lower().endswith('.pdf'):
        logger.error(f"The file {sanitized_pdf_path} is not a valid PDF file.")
        raise ValueError(f"The file {sanitized_pdf_path} is not a valid PDF file.")
        
    document = pymupdf.open(sanitized_pdf_path)
    if document.page_count == 0:
        logger.error(f"The file {sanitized_pdf_path} is empty or has no pages.")
        raise ValueError("The PDF file is empty or has no pages.")

    images = []

    for page_number in range(document.page_count):
        logger.debug(f"Converting page {page_number} to image")
        page = document.load_page(page_number)
        pix = page.get_pixmap()
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        images.append(img)

    logger.debug(f"Converted {len(images)} pages to images")
    return images