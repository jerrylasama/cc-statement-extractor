from pathlib import Path
from typing import List, Optional, TYPE_CHECKING

from cc_statement_extractor.shared.logger import logger

if TYPE_CHECKING:
    from paddleocr import PaddleOCRVL

def run_ocr(pdf_path: Path, output_path: Path, pipeline: 'PaddleOCRVL', force: bool = False) -> Optional[Path]:
    """Runs OCR and saves the uncensored markdown. Returns the path if successful."""
    file_stem = pdf_path.stem
    anonymized_path = output_path / f"{file_stem}-anonymized.md"
    generated_md_path = output_path / f"{file_stem}.md"

    if anonymized_path.exists() and not force:
        logger.info(f"Skipping {pdf_path.name} as it already exists: {anonymized_path}")
        return None

    if generated_md_path.exists() and not force:
        logger.info(f"Skipping OCR for {pdf_path.name} as uncensored markdown already exists: {generated_md_path}")
        return generated_md_path

    logger.info(f"Starting OCR extraction for {pdf_path}...")

    try:
        results = pipeline.predict(str(pdf_path))
        page_res: List = list(results)
        if not page_res:
            logger.warning(f"No content extracted from {pdf_path}")
            return None

        structured_output = pipeline.restructure_pages(
            page_res, 
            merge_tables=True,
            relevel_titles=True,
            concatenate_pages=True,
        )

        for res in structured_output:
            logger.info("Processing result page/document...")
            try:
                res.save_to_markdown(save_path=str(output_path))
                logger.info(f"Successfully saved OCR data to {output_path}")
            except Exception as e:
                logger.error(f"Failed to save markdown for {pdf_path.name}: {e}")
                raise

        if not generated_md_path.exists():
            raise FileNotFoundError(f"Expected output file {generated_md_path} not found after OCR.")
            
        return generated_md_path

    except Exception as e:
        logger.error(f"Error extracting {pdf_path.name}: {e}", exc_info=True)
        return None
