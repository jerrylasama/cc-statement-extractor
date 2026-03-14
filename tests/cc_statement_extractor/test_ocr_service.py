import pytest
from unittest.mock import MagicMock

from cc_statement_extractor.ocr.service import run_ocr

@pytest.fixture
def mock_pipeline():
    return MagicMock()

@pytest.fixture
def test_paths(tmp_path):
    pdf_path = tmp_path / "test.pdf"
    output_path = tmp_path / "output"
    output_path.mkdir(exist_ok=True)
    return pdf_path, output_path

def test_run_ocr_skips_when_anonymized_exists(mock_pipeline, test_paths):
    pdf_path, output_path = test_paths
    anonymized_path = output_path / "test-anonymized.md"
    anonymized_path.touch()
    
    result = run_ocr(pdf_path, output_path, mock_pipeline, force=False)
    assert result is None
    mock_pipeline.predict.assert_not_called()

def test_run_ocr_skips_when_generated_md_exists(mock_pipeline, test_paths):
    pdf_path, output_path = test_paths
    generated_md_path = output_path / "test.md"
    generated_md_path.touch()
    
    result = run_ocr(pdf_path, output_path, mock_pipeline, force=False)
    assert result == generated_md_path
    mock_pipeline.predict.assert_not_called()

def test_run_ocr_forces_when_anonymized_exists(mock_pipeline, test_paths):
    pdf_path, output_path = test_paths
    anonymized_path = output_path / "test-anonymized.md"
    anonymized_path.touch()
    generated_md_path = output_path / "test.md"
    
    mock_pipeline.predict.return_value = ["page1"]
    mock_res = MagicMock()
    mock_pipeline.restructure_pages.return_value = [mock_res]
    
    def touch_md(*args, **kwargs):
        generated_md_path.touch()
    
    mock_res.save_to_markdown.side_effect = touch_md
    
    result = run_ocr(pdf_path, output_path, mock_pipeline, force=True)
    assert result == generated_md_path
    mock_pipeline.predict.assert_called_once()
    mock_res.save_to_markdown.assert_called_once()


def test_run_ocr_no_content_extracted(mock_pipeline, test_paths):
    pdf_path, output_path = test_paths
    
    mock_pipeline.predict.return_value = []
    
    result = run_ocr(pdf_path, output_path, mock_pipeline)
    assert result is None
    mock_pipeline.restructure_pages.assert_not_called()

def test_run_ocr_exception_during_save(mock_pipeline, test_paths):
    pdf_path, output_path = test_paths
    
    mock_pipeline.predict.return_value = ["page1"]
    mock_res = MagicMock()
    mock_pipeline.restructure_pages.return_value = [mock_res]
    
    mock_res.save_to_markdown.side_effect = IOError("Simulated IO Error")
    
    result = run_ocr(pdf_path, output_path, mock_pipeline)
    assert result is None

def test_run_ocr_file_not_found_after_processing(mock_pipeline, test_paths):
    pdf_path, output_path = test_paths
    
    mock_pipeline.predict.return_value = ["page1"]
    mock_res = MagicMock()
    mock_pipeline.restructure_pages.return_value = [mock_res]
    
    # Do not create the expected generated_md_path output 
    result = run_ocr(pdf_path, output_path, mock_pipeline)
    assert result is None
    mock_res.save_to_markdown.assert_called_once()

def test_run_ocr_general_predict_exception(mock_pipeline, test_paths):
    pdf_path, output_path = test_paths
    mock_pipeline.predict.side_effect = RuntimeError("PaddleOCR failure")
    
    result = run_ocr(pdf_path, output_path, mock_pipeline)
    assert result is None
