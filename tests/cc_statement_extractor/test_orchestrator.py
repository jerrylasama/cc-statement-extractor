import pytest
import sys
import asyncio
import concurrent.futures
from pathlib import Path
from unittest.mock import MagicMock, patch

from cc_statement_extractor.pipeline.orchestrator import (
    producer,
    consumer,
    get_files_to_process,
    run_pipeline
)

@pytest.fixture
def mock_args():
    args = MagicMock()
    args.file_name = None
    args.force = False
    args.dry_run = False
    return args

@pytest.fixture
def test_dirs(tmp_path):
    data_path = tmp_path / "data"
    output_path = tmp_path / "output"
    data_path.mkdir()
    output_path.mkdir()
    return data_path, output_path

@pytest.mark.asyncio
async def test_producer():
    queue = asyncio.Queue()
    files = [Path("file1.pdf"), Path("file2.pdf")]
    pipeline = MagicMock()
    output_path = Path("out")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        with patch("cc_statement_extractor.pipeline.orchestrator.run_ocr") as mock_run_ocr:
            # First file is successful (returns md_path), second fails (returns None)
            mock_run_ocr.side_effect = [Path("out/file1.md"), None]
            
            await producer(queue, files, executor, pipeline, force=False, output_path=output_path)
            
            # Queue should have file1.md and then None (sentinel)
            assert queue.qsize() == 2
            item1 = await queue.get()
            assert item1 == Path("out/file1.md")
            item2 = await queue.get()
            assert item2 is None
            
            assert mock_run_ocr.call_count == 2

@pytest.mark.asyncio
async def test_consumer():
    queue = asyncio.Queue()
    config = MagicMock()
    
    # Put some items in queue
    await queue.put(Path("out/file1.md"))
    await queue.put(Path("out/file2.md"))
    await queue.put(None)  # Sentinel
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        with patch("cc_statement_extractor.pipeline.orchestrator.run_pii") as mock_run_pii:
            mock_run_pii.side_effect = [True, False]
            
            with patch("cc_statement_extractor.pipeline.orchestrator.logger.error") as mock_logger_error:
                await consumer(queue, executor, config)
                
                assert mock_run_pii.call_count == 2
                mock_logger_error.assert_called_once()
                assert "Consumer failed to process file2.md" in mock_logger_error.call_args[0][0]

@pytest.mark.asyncio
async def test_async_main_cuda_fallback():
    mock_paddle = MagicMock()
    mock_paddle.device.is_compiled_with_cuda.return_value = True
    mock_paddleocr_mod = MagicMock()
    mock_paddleocr_cls = MagicMock()
    mock_paddleocr_mod.PaddleOCRVL = mock_paddleocr_cls
    
    # Needs a new patch for PaddleOCRVL
    call_count = {"count": 0}
    def fake_init(*args, **kwargs):
        if "precision" in kwargs and call_count["count"] == 0:
            call_count["count"] += 1
            raise TypeError("got an unexpected keyword argument 'precision'")
        return MagicMock()
    mock_paddleocr_cls.side_effect = fake_init

    args = MagicMock()
    args.force = False
    files_to_process = []
    config = MagicMock()
    config.get.return_value = []
    output_path = Path("out")
    
    with patch.dict(sys.modules, {"paddle": mock_paddle, "paddleocr": mock_paddleocr_mod}):
        with patch("cc_statement_extractor.pipeline.orchestrator.producer") as mock_prod, \
             patch("cc_statement_extractor.pipeline.orchestrator.consumer") as mock_cons:
            from cc_statement_extractor.pipeline.orchestrator import async_main
            await async_main(args, files_to_process, config, output_path)
            
    # Ensure it was called twice
    assert mock_paddleocr_cls.call_count == 2
    mock_prod.assert_called_once()
    mock_cons.assert_called_once()

def test_get_files_to_process_specific_file(mock_args, test_dirs):
    data_path, output_path = test_dirs
    
    # 1. file_name exists with .pdf
    mock_args.file_name = "test.pdf"
    (data_path / "test.pdf").touch()
    
    files = get_files_to_process(mock_args, data_path, output_path)
    assert len(files) == 1
    assert files[0].name == "test.pdf"
    
    # 2. file_name exists without .pdf (should auto-append)
    mock_args.file_name = "test"
    files = get_files_to_process(mock_args, data_path, output_path)
    assert len(files) == 1
    assert files[0].name == "test.pdf"
    
    # 3. file_name does not exist at all
    mock_args.file_name = "nonexistent"
    files = get_files_to_process(mock_args, data_path, output_path)
    assert len(files) == 0

def test_get_files_to_process_directory(mock_args, test_dirs):
    data_path, output_path = test_dirs
    mock_args.file_name = None
    
    (data_path / "f1.pdf").touch()
    (data_path / "f2.pdf").touch()
    (data_path / "f3.pdf").touch()
    
    # f1 is anonymized already
    (output_path / "f1-anonymized.md").touch()
    
    # force = False
    mock_args.force = False
    files = get_files_to_process(mock_args, data_path, output_path)
    assert len(files) == 2
    assert "f2.pdf" in [f.name for f in files]
    assert "f3.pdf" in [f.name for f in files]
    
    # force = True
    mock_args.force = True
    files = get_files_to_process(mock_args, data_path, output_path)
    assert len(files) == 3

@patch("cc_statement_extractor.pipeline.orchestrator.Config")
@patch("cc_statement_extractor.pipeline.orchestrator.asyncio.run")
def test_run_pipeline_flow(mock_asyncio_run, mock_config_cls, mock_args, test_dirs):
    data_path, output_path = test_dirs
    mock_config = mock_config_cls.return_value
    
    # 1. Validation fails
    mock_config.validate.side_effect = ValueError("Invalid config")
    run_pipeline(mock_args)
    mock_asyncio_run.assert_not_called()
    
    mock_config.validate.side_effect = None

    
    # 2. No files to process
    with patch("cc_statement_extractor.pipeline.orchestrator.get_files_to_process", return_value=[]), \
         patch("cc_statement_extractor.pipeline.orchestrator.Path.exists", return_value=False), \
         patch("cc_statement_extractor.pipeline.orchestrator.Path.mkdir") as mock_mkdir:
         
        run_pipeline(mock_args)
        mock_asyncio_run.assert_not_called()
        mock_mkdir.assert_called_once()
        
    # 3. Dry run
    mock_args.dry_run = True
    with patch("cc_statement_extractor.pipeline.orchestrator.get_files_to_process", return_value=[Path("test.pdf")]):
        run_pipeline(mock_args)
        mock_asyncio_run.assert_not_called()
        
    # 4. Happy path
    mock_args.dry_run = False
    with patch("cc_statement_extractor.pipeline.orchestrator.get_files_to_process", return_value=[Path("test.pdf")]):
        run_pipeline(mock_args)
        mock_asyncio_run.assert_called_once()
