import pytest
from unittest.mock import MagicMock, patch

from cc_statement_extractor.pii.presidio import _load_entity_mapping, pii_anonymizer

def test_load_entity_mapping_json_error(tmp_path):
    mapping_file = tmp_path / "entity.map.json"
    mapping_file.write_text("{invalid_json}")
    with patch("cc_statement_extractor.pii.presidio.logger") as mock_logger:
        mapping = _load_entity_mapping(mapping_file)
        assert mapping == {}
        mock_logger.error.assert_called()

def test_load_entity_mapping_general_error(tmp_path):
    mapping_file = tmp_path / "entity.map.json"
    mapping_file.write_text("{}") # Valid json
    with patch("builtins.open", side_effect=Exception("General err")):
        with patch("cc_statement_extractor.pii.presidio.logger") as mock_logger:
            mapping = _load_entity_mapping(mapping_file)
            assert mapping == {}
            mock_logger.error.assert_called()

@pytest.fixture
def mock_config():
    config = MagicMock()
    config.get.return_value = None
    return config

def test_pii_anonymizer_not_a_file(tmp_path):
    with patch("cc_statement_extractor.pii.presidio.logger") as mock_logger:
        pii_anonymizer(tmp_path, MagicMock())
        mock_logger.error.assert_called_with(f"The path {tmp_path} is not a file.")

def test_pii_anonymizer_read_exception(tmp_path, mock_config):
    test_file = tmp_path / "test.md"
    test_file.touch()
    
    with patch("builtins.open", side_effect=IOError("Read err")):
        with patch("cc_statement_extractor.pii.presidio.logger") as mock_logger:
            pii_anonymizer(test_file, mock_config)
            mock_logger.error.assert_called()


