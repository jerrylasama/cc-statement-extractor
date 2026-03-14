import pytest
from unittest.mock import MagicMock, patch

from cc_statement_extractor.pii.service import run_pii


@pytest.fixture
def mock_config():
    return MagicMock()

def test_run_pii_success(tmp_path, mock_config):
    md_path = tmp_path / "test.md"
    md_path.write_text("some content")
    
    with patch("cc_statement_extractor.pii.service.logger") as mock_logger:
        with patch("cc_statement_extractor.pii.presidio.pii_anonymizer") as mock_anonymizer:
            result = run_pii(md_path, mock_config)
            
            assert result is True
            mock_anonymizer.assert_called_once_with(md_path, mock_config)
            assert not md_path.exists()
            assert "Deleted uncensored markdown file" in mock_logger.info.call_args_list[-1][0][0]

def test_run_pii_cleanup_failure(tmp_path, mock_config):
    md_path = tmp_path / "test.md"
    md_path.write_text("some content")
    
    with patch("cc_statement_extractor.pii.service.logger") as mock_logger:
        with patch("cc_statement_extractor.pii.presidio.pii_anonymizer") as mock_anonymizer:
            with patch("pathlib.Path.unlink", side_effect=PermissionError("Cannot delete")):
                result = run_pii(md_path, mock_config)
                
                assert result is True
                mock_anonymizer.assert_called_once()
                mock_logger.warning.assert_called_once()
                assert "Failed to delete uncensored markdown file" in mock_logger.warning.call_args[0][0]

def test_run_pii_anonymization_failure(tmp_path, mock_config):
    md_path = tmp_path / "test.md"
    md_path.write_text("some content")
    
    with patch("cc_statement_extractor.pii.service.logger") as mock_logger:
        with patch("cc_statement_extractor.pii.presidio.pii_anonymizer", side_effect=Exception("Anonymization failed")):
            result = run_pii(md_path, mock_config)
            
            assert result is False
            mock_logger.error.assert_called_once()
            assert "PII anonymization failed" in mock_logger.error.call_args[0][0]
            assert md_path.exists() # Verify file is untouched upon failure
