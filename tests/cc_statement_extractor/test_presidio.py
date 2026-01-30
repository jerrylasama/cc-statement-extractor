import pytest
from unittest.mock import MagicMock, patch, mock_open
from presidio_analyzer import RecognizerResult
from cc_statement_extractor.pii.presidio import pii_anonymizer, _load_entity_mapping

@pytest.fixture
def mock_config():
    config = MagicMock()
    def get_side_effect(key, default=None):
        config_data = {
            "nlp.engine": "spacy",
            "nlp.models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
            "pii.custom_deny_list": ["SECRET_KEY"],
            "pii.target_entities": ["PERSON", "PHONE_NUMBER"],
            "nlp.analyzer_lang": "en"
        }
        return config_data.get(key, default)
    
    config.get.side_effect = get_side_effect
    return config

def test_load_entity_mapping_file_not_found():
    with patch("pathlib.Path.exists", return_value=False):
        mapping = _load_entity_mapping("non-existent.json")
        assert mapping == {}

def test_load_entity_mapping_success():
    mock_json = '{"PERSON": "NAMA", "LOCATION": "TEMPAT"}'
    with patch("pathlib.Path.exists", return_value=True):
        with patch("builtins.open", mock_open(read_data=mock_json)):
            mapping = _load_entity_mapping("entity.map.json")
            assert mapping == {"PERSON": "NAMA", "LOCATION": "TEMPAT"}

@patch("cc_statement_extractor.pii.presidio.AnalyzerEngine")
@patch("cc_statement_extractor.pii.presidio.AnonymizerEngine")
@patch("cc_statement_extractor.pii.presidio.logger")
@patch("pathlib.Path.exists", return_value=True)
@patch("pathlib.Path.is_file", return_value=True)
@patch("builtins.open", new_callable=mock_open, read_data="My name is John Doe and my NIK is 1234567890123456.")
def test_pii_anonymizer_success(mock_file, mock_exists, mock_is_file, mock_logger, mock_anonymizer_class, mock_analyzer_class, mock_config):
    mock_analyzer = mock_analyzer_class.return_value
    mock_anonymizer = mock_anonymizer_class.return_value
    
    result_person = RecognizerResult(entity_type="PERSON", start=11, end=19, score=0.9)
    result_nik = RecognizerResult(entity_type="ID_NIK", start=34, end=50, score=0.95)
    mock_analyzer.analyze.return_value = [result_person, result_nik]
    
    mock_anonymized_result = MagicMock()
    mock_anonymized_result.text = "My name is REDACTED and my NIK is REDACTED."
    mock_anonymizer.anonymize.return_value = mock_anonymized_result
    
    pii_anonymizer("test.md", mock_config)
    
    mock_analyzer.analyze.assert_called_once()
    mock_anonymizer.anonymize.assert_called_once()
    
    write_call = [call for call in mock_file.call_args_list if call.args[1] == 'w']
    assert len(write_call) == 1
    mock_file().write.assert_called_with("My name is REDACTED and my NIK is REDACTED.")
    mock_logger.info.assert_called_with("Successfully anonymized text to test-anonymized.md")

@patch("cc_statement_extractor.pii.presidio.logger")
@patch("pathlib.Path.exists", return_value=False)
def test_pii_anonymizer_file_not_found(mock_exists, mock_logger, mock_config):
    pii_anonymizer("non-existent.md", mock_config)
    mock_logger.error.assert_called_with("The path non-existent.md does not exist.")

@patch("pathlib.Path.exists", return_value=True)
@patch("pathlib.Path.is_file", return_value=True)
@patch("builtins.open", new_callable=mock_open, read_data="")
@patch("cc_statement_extractor.pii.presidio.logger")
def test_pii_anonymizer_empty_file(mock_logger, mock_file, mock_is_file, mock_exists, mock_config):
    pii_anonymizer("empty.md", mock_config)
    mock_logger.warning.assert_called_with("File empty.md is empty. Skipping anonymization.")

@patch("cc_statement_extractor.pii.presidio.AnalyzerEngine")
@patch("cc_statement_extractor.pii.presidio.AnonymizerEngine")
@patch("cc_statement_extractor.pii.presidio.logger")
@patch("pathlib.Path.exists", return_value=True)
@patch("pathlib.Path.is_file", return_value=True)
@patch("builtins.open", new_callable=mock_open, read_data="Sample text")
def test_pii_anonymizer_custom_recognizers(mock_file, mock_exists, mock_is_file, mock_logger, mock_anonymizer_class, mock_analyzer_class, mock_config):
    mock_analyzer = mock_analyzer_class.return_value
    
    pii_anonymizer("test.md", mock_config)
    
    calls = []
    for call in mock_analyzer.registry.add_recognizer.call_args_list:
        recognizer = call.args[0]
        if hasattr(recognizer, 'supported_entities'):
            calls.extend(recognizer.supported_entities)
        elif hasattr(recognizer, 'supported_entity'):
            calls.append(recognizer.supported_entity)
            
    assert "ID_NIK" in calls
    assert "PREMASKED_CC_NO" in calls
    assert "CUSTOM_DENY_LIST" in calls
