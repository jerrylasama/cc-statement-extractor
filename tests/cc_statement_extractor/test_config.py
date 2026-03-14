import pytest
import yaml
from cc_statement_extractor.shared.config import Config

@pytest.fixture
def temp_config_file(tmp_path):
    config_data = {
        "huggingface": {
            "model": "test/test-01",
            "api_key": "hf_unmaskedTokenKey",
            "endpoints": ["https://api1.hf.co", "https://api2.hf.co"]
        },
        "allowed_entities": ["PERSON", "LOCATION", "ORG"]
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)
    return str(config_file)

def test_nested_config(temp_config_file):
    config = Config(temp_config_file)
    
    model = config.get("huggingface.model")
    assert model == "test/test-01"
    
    api_key = config.get("huggingface.api_key")
    assert api_key == "hf_unmaskedTokenKey"
    
    non_existent = config.get("huggingface.nonexistent", "default_val")
    assert non_existent == "default_val"
    
    config.set("new.nested.key", "test_value")
    new_val = config.get("new.nested.key")
    assert new_val == "test_value"
    
    entities = config.get("allowed_entities")
    assert isinstance(entities, list)
    assert "PERSON" in entities
    assert len(entities) == 3

    endpoints = config.get("huggingface.endpoints")
    assert isinstance(endpoints, list)
    assert len(endpoints) == 2
    assert endpoints[0] == "https://api1.hf.co"

    config.set("new.list", [1, 2, 3])
    assert config.get("new.list") == [1, 2, 3]

    config.set("huggingface.model", "Test01")
    updated_model = config.get("huggingface.model")
    assert updated_model == "Test01"

def test_config_save(temp_config_file):
    config = Config(temp_config_file)
    config.set("new.key", "saved_value")
    config.save()
    
    new_config = Config(temp_config_file)
    assert new_config.get("new.key") == "saved_value"


def test_config_not_found():
    config = Config("nonexistent_path.yaml")
    assert config.config == {}
    with pytest.raises(FileNotFoundError):
        config.validate()

def test_validate_empty_config(tmp_path):
    empty_file = tmp_path / "empty.yaml"
    empty_file.touch()
    config = Config(str(empty_file))
    with pytest.raises(ValueError, match="empty or malformed"):
        config.validate()

def test_validate_missing_keys(temp_config_file):
    config = Config(temp_config_file)
    with pytest.raises(ValueError, match="missing required top-level keys"):
        config.validate()

def test_validate_success(tmp_path):
    config_data = {"ocr": {}, "nlp": {}, "pii": {}}
    valid_file = tmp_path / "valid.yaml"
    with open(valid_file, "w") as f:
        yaml.dump(config_data, f)
    config = Config(str(valid_file))
    config.validate()
