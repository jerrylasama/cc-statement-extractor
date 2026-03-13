from __future__ import annotations
import argparse
import asyncio
import concurrent.futures
import os
from pathlib import Path
from typing import List, Optional, TYPE_CHECKING

from cc_statement_extractor.shared.config import Config
from cc_statement_extractor.shared.logger import logger

if TYPE_CHECKING:
    from paddleocr import PaddleOCRVL

config: Config = Config()
DATA_PATH: Path = Path("data")
OUTPUT_PATH: Path = Path("output")

def run_ocr(pdf_path: Path, pipeline: PaddleOCRVL, force: bool = False) -> Optional[Path]:
    """Runs OCR and saves the uncensored markdown. Returns the path if successful."""
    file_stem = pdf_path.stem
    anonymized_path = OUTPUT_PATH / f"{file_stem}-anonymized.md"
    generated_md_path = OUTPUT_PATH / f"{file_stem}.md"

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
                res.save_to_markdown(save_path=str(OUTPUT_PATH))
                logger.info(f"Successfully saved OCR data to {OUTPUT_PATH}")
            except Exception as e:
                logger.error(f"Failed to save markdown for {pdf_path.name}: {e}")
                raise

        if not generated_md_path.exists():
            raise FileNotFoundError(f"Expected output file {generated_md_path} not found after OCR.")
            
        return generated_md_path

    except Exception as e:
        logger.error(f"Error extracting {pdf_path.name}: {e}", exc_info=True)
        return None

def run_pii(generated_md_path: Path) -> bool:
    """Runs PII anonymization on a markdown file."""
    logger.info(f"Anonymizing PII in {generated_md_path}...")
    try:
        from cc_statement_extractor.pii.presidio import pii_anonymizer
        pii_anonymizer(generated_md_path, config)
        logger.info(f"Successfully anonymized PII for {generated_md_path.name}")
        
        # Cleanup uncensored MD
        try:
            generated_md_path.unlink()
            logger.info(f"Deleted uncensored markdown file {generated_md_path}")
        except Exception as e:
            logger.warning(f"Failed to delete uncensored markdown file {generated_md_path}: {e}")
            
        return True
    except Exception as e:
        logger.error(f"PII anonymization failed for {generated_md_path.name}: {e}")
        return False

async def producer(queue: asyncio.Queue, files: List[Path], executor: concurrent.futures.ThreadPoolExecutor, pipeline: PaddleOCRVL, force: bool):
    """Producer task: runs OCR and pushes MD paths to the queue."""
    for pdf_path in files:
        # Run OCR in the thread pool to avoid blocking the event loop
        md_path = await asyncio.get_running_loop().run_in_executor(
            executor,
            run_ocr,
            pdf_path,
            pipeline,
            force
        )
        if md_path:
            await queue.put(md_path)
    
    # Send sentinel to consumer indicating there's no more work
    await queue.put(None)

async def consumer(queue: asyncio.Queue, executor: concurrent.futures.ThreadPoolExecutor):
    """Consumer task: grabs MD paths from the queue and runs PII anonymization."""
    while True:
        md_path = await queue.get()
        if md_path is None:
            queue.task_done()
            break
            
        # Run PII in the thread pool
        success = await asyncio.get_running_loop().run_in_executor(
            executor,
            run_pii,
            md_path
        )
        
        if not success:
            logger.error(f"Consumer failed to process {md_path.name}. File has been preserved for manual inspection.")
            
        queue.task_done()

async def async_main(args: argparse.Namespace, files_to_process: List[Path]) -> None:
    """Async orchestration of producer and consumer pipelines."""
    # Determine safe worker count leaving room for OS operations (min 1, max os.cpu_count() - 2)
    cpu_count = os.cpu_count() or 4
    max_workers = max(1, cpu_count - 2)
    logger.info(f"Setting ThreadPoolExecutor max_workers to: {max_workers}")
    
    logger.info("Initializing PaddleOCRVL...")
    from paddleocr import PaddleOCRVL
    import paddle
    
    use_fp16 = False
    if paddle.device.is_compiled_with_cuda():
        # Heuristic for mixed precision on GPU
        logger.info("CUDA detected: Enabling fp16 mixed precision for inference.")
        use_fp16 = True

    try:
        if use_fp16:
            pipeline = PaddleOCRVL(
                markdown_ignore_labels=config.get("ocr.markdown_ignore_labels", []),
                precision="fp16"
            )
        else:
            pipeline = PaddleOCRVL(
                markdown_ignore_labels=config.get("ocr.markdown_ignore_labels", [])
            )
    except TypeError:
        logger.warning("PaddleOCRVL does not support precision kwarg. Falling back to default.")
        pipeline = PaddleOCRVL(
            markdown_ignore_labels=config.get("ocr.markdown_ignore_labels", [])
        )

    queue = asyncio.Queue()
    
    # Use ThreadPoolExecutor for blocking synchronous NLP/Vision operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        producer_task = asyncio.create_task(producer(queue, files_to_process, executor, pipeline, args.force))
        consumer_task = asyncio.create_task(consumer(queue, executor))
        
        # Wait for producer to finish creating work and consumer to finish processing sentinels
        await asyncio.gather(producer_task, consumer_task)
    
    logger.info("Execution pipeline completed.")

def main() -> None:
    """
    Main entry point for the credit card statement extraction pipeline.
    This function performs OCR on PDF files asynchronously, queues the results,
    and then anonymizes any PII found in the temporary markdown files.
    """
    parser = argparse.ArgumentParser(description="Extract and anonymize CC statements.")
    parser.add_argument("-f", "--file_name", type=str, help="Process only this file (example: 2401.pdf or 2401)")
    parser.add_argument("--force", action="store_true", help="Force processing even if anonymized file exists")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Simulate execution without moving data or extracting")
    args = parser.parse_args()

    try:
        config.validate()
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return

    if not OUTPUT_PATH.exists():
        logger.info(f"Creating output directory: {OUTPUT_PATH}")
        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    try:
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
            all_pdfs = sorted(list(DATA_PATH.glob("*.pdf")))
            if not args.force:
                for pdf_path in all_pdfs:
                    anonymized_path = OUTPUT_PATH / f"{pdf_path.stem}-anonymized.md"
                    if not anonymized_path.exists():
                        files_to_process.append(pdf_path)
            else:
                files_to_process = all_pdfs

        if not files_to_process:
            logger.warning(f"No PDF files found to process in {DATA_PATH} (all might be skipped).")
            return

        logger.info(f"Found {len(files_to_process)} files to process.")

        if args.dry_run:
            logger.info("Dry run mode enabled. Simulating processing...")
            for pdf_path in files_to_process:
                logger.info(f"[DRY RUN] Would queue {pdf_path.name} for OCR and PII processing")
            return

        asyncio.run(async_main(args, files_to_process))

    except Exception as e:
        logger.critical(f"A critical error occurred in the extraction pipeline: {e}", exc_info=True)

if __name__ == "__main__":
    main()