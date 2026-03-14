import argparse
import asyncio
import concurrent.futures
import os
from pathlib import Path
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from paddleocr import PaddleOCRVL

from cc_statement_extractor.shared.config import Config
from cc_statement_extractor.shared.logger import logger
from cc_statement_extractor.ocr.service import run_ocr
from cc_statement_extractor.pii.service import run_pii

async def producer(queue: asyncio.Queue, files: List[Path], executor: concurrent.futures.ThreadPoolExecutor, pipeline: 'PaddleOCRVL', force: bool, output_path: Path):
    """Producer task: runs OCR and pushes MD paths to the queue."""
    for pdf_path in files:
        # Run OCR in the thread pool to avoid blocking the event loop
        md_path = await asyncio.get_running_loop().run_in_executor(
            executor,
            run_ocr,
            pdf_path,
            output_path,
            pipeline,
            force
        )
        if md_path:
            await queue.put(md_path)
    
    # Send sentinel to consumer indicating there's no more work
    await queue.put(None)

async def consumer(queue: asyncio.Queue, executor: concurrent.futures.ThreadPoolExecutor, config: Config):
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
            md_path,
            config
        )
        
        if not success:
            logger.error(f"Consumer failed to process {md_path.name}. File has been preserved for manual inspection.")
            
        queue.task_done()

async def async_main(args: argparse.Namespace, files_to_process: List[Path], config: Config, output_path: Path) -> None:
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
        producer_task = asyncio.create_task(producer(queue, files_to_process, executor, pipeline, args.force, output_path))
        consumer_task = asyncio.create_task(consumer(queue, executor, config))
        
        # Wait for producer to finish creating work and consumer to finish processing sentinels
        await asyncio.gather(producer_task, consumer_task)
    
    logger.info("Execution pipeline completed.")

def get_files_to_process(args: argparse.Namespace, data_path: Path, output_path: Path) -> List[Path]:
    files_to_process: List[Path] = []
    if args.file_name:
        # Handle both with and without .pdf extension
        fname = args.file_name
        if not fname.lower().endswith(".pdf"):
            fname += ".pdf"
        pdf_path = data_path / fname
        if pdf_path.exists():
            files_to_process.append(pdf_path)
        else:
            logger.error(f"Specified file not found: {pdf_path}")
    else:
        all_pdfs = sorted(list(data_path.glob("*.pdf")))
        if not args.force:
            for pdf_path in all_pdfs:
                anonymized_path = output_path / f"{pdf_path.stem}-anonymized.md"
                if not anonymized_path.exists():
                    files_to_process.append(pdf_path)
        else:
            files_to_process = all_pdfs
    return files_to_process

def run_pipeline(args: argparse.Namespace) -> None:
    config: Config = Config()
    try:
        config.validate()
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return

    DATA_PATH: Path = Path("data")
    OUTPUT_PATH: Path = Path("output")

    if not OUTPUT_PATH.exists():
        logger.info(f"Creating output directory: {OUTPUT_PATH}")
        OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    files_to_process = get_files_to_process(args, DATA_PATH, OUTPUT_PATH)

    if not files_to_process:
        logger.warning(f"No PDF files found to process in {DATA_PATH} (all might be skipped).")
        return

    logger.info(f"Found {len(files_to_process)} files to process.")

    if args.dry_run:
        logger.info("Dry run mode enabled. Simulating processing...")
        for pdf_path in files_to_process:
            logger.info(f"[DRY RUN] Would queue {pdf_path.name} for OCR and PII processing")
        return

    asyncio.run(async_main(args, files_to_process, config, OUTPUT_PATH))
