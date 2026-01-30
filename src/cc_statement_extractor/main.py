import argparse
from typing import List, Optional
from pathlib import Path

from cc_statement_extractor.shared.config import Config
from cc_statement_extractor.shared.logger import logger
from cc_statement_extractor.pii.presidio import pii_anonymizer

from paddleocr import PaddleOCRVL

config: Config = Config()
DATA_PATH: Path = Path("data")
OUTPUT_PATH: Path = Path("output")

def process_single_file(pdf_path: Path, pipeline: PaddleOCRVL, force: bool = False) -> None:
    """
    Process a single PDF file: OCR -> Markdown -> Anonymize.
    """
    file_stem = pdf_path.stem
    anonymized_path = OUTPUT_PATH / f"{file_stem}-anonymized.md"

    if anonymized_path.exists() and not force:
        logger.info(f"Skipping {pdf_path.name} as it already exists: {anonymized_path}")
        return

    logger.info(f"Starting extraction for {pdf_path}...")

    try:
        results = pipeline.predict(str(pdf_path))
        
        page_res: List = list(results)
        if not page_res:
            logger.warning(f"No content extracted from {pdf_path}")
            return

        structured_output = pipeline.restructure_pages(
            page_res, 
            merge_tables=True,
            relevel_titles=True,
            concatenate_pages=True,
        )

        success = False
        generated_md_path: Optional[Path] = None

        for res in structured_output:
            logger.info("Processing result page/document...")
            
            try:
                res.save_to_markdown(save_path=str(OUTPUT_PATH))
                logger.info(f"Successfully saved data to {OUTPUT_PATH}")
            except Exception as e:
                logger.error(f"Failed to save markdown: {e}")
                raise

            generated_md_path = Path(OUTPUT_PATH / f"{file_stem}.md")
            if not generated_md_path.exists():
                raise FileNotFoundError(f"Expected output file {generated_md_path} not found for anonymization")

            logger.info(f"Anonymizing PII in {generated_md_path}...")
            try:
                pii_anonymizer(generated_md_path, config)
                logger.info("Successfully anonymized PII")
                success = True
            except Exception as e:
                logger.error(f"PII anonymization failed: {e}")
                raise

        if success and generated_md_path and generated_md_path.exists():
            try:
                generated_md_path.unlink()
                logger.info(f"Deleted uncensored markdown file {generated_md_path}")
            except Exception as e:
                logger.warning(f"Failed to delete uncensored markdown file {generated_md_path}: {e}")
        elif not success:
            logger.warning(f"Processing did not complete successfully for {pdf_path.name}")

    except Exception as e:
        logger.error(f"Error processing {pdf_path.name}: {e}", exc_info=True)

def main() -> None:
    """
    Main entry point for the credit card statement extraction pipeline.
    This function performs OCR on PDF files, saves the results as markdown,
    and then anonymizes any PII found in the markdown.
    """
    parser = argparse.ArgumentParser(description="Extract and anonymize CC statements.")
    parser.add_argument("-f", "--file_name", type=str, help="Process only this file (example: 2401.pdf or 2401)")
    parser.add_argument("--force", action="store_true", help="Force processing even if anonymized file exists")
    args = parser.parse_args()

    if not OUTPUT_PATH.exists():
        logger.info(f"Creating output directory: {OUTPUT_PATH}")
        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    try:
        logger.info("Initializing PaddleOCRVL...")
        pipeline: PaddleOCRVL = PaddleOCRVL(
            markdown_ignore_labels=config.get("ocr.markdown_ignore_labels", [])
        )

        files_to_process: List[Path] = []
        if args.file_name:
            # Handle both with and without .pdf extension
            fname = args.file_name
            if not fname.lower().endswith(".pdf"):
                fname += ".pdf"
            pdf_path = DATA_PATH / fname
            if pdf_path.exists():
                files_to_process.append(pdf_path)
            else:
                logger.error(f"Specified file not found: {pdf_path}")
                return
        else:
            files_to_process = sorted(list(DATA_PATH.glob("*.pdf")))

        if not files_to_process:
            logger.warning(f"No PDF files found to process in {DATA_PATH}")
            return

        logger.info(f"Found {len(files_to_process)} files to process.")

        for pdf_path in files_to_process:
            process_single_file(pdf_path, pipeline, args.force)

    except Exception as e:
        logger.critical(f"A critical error occurred in the extraction pipeline: {e}", exc_info=True)

if __name__ == "__main__":
    main()