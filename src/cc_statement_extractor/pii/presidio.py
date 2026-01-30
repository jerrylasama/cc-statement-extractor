from typing import Dict, Union
from pathlib import Path
import json

from presidio_analyzer import (
    AnalyzerEngine, 
    PatternRecognizer, 
    Pattern
)
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from presidio_analyzer.predefined_recognizers import GLiNERRecognizer

from cc_statement_extractor.shared.logger import logger
from cc_statement_extractor.shared.config import Config

def _load_entity_mapping(mapping_path: Union[str, Path] = "entity.map.json") -> Dict[str, str]:
    """
    Load entity mapping from a JSON file.
    
    Args:
        mapping_path: Path to the entity map JSON file.
        
    Returns:
        Dict mapping source entities to target entities.
    """
    try:
        path = Path(mapping_path)
        if not path.exists():
            logger.warning(f"Entity mapping file {mapping_path} not found. Using empty mapping.")
            return {}
            
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode entity mapping file {mapping_path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error loading entity mapping: {e}")
        return {}

def pii_anonymizer(file_path: Union[str, Path], config: Config) -> None:
    """
    Anonymize PII in a file and save the result.
    
    Args:
        file_path: Path to the markdown file to anonymize.
        config: Config object containing PII and NLP settings.
    """
    try:
        path = Path(file_path)
        if not path.exists():
            logger.error(f"The path {file_path} does not exist.")
            return

        if not path.is_file():
            logger.error(f"The path {file_path} is not a file.")
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return

        if not text:
            logger.warning(f"File {file_path} is empty. Skipping anonymization.")
            return

        # Initialize NLP Engine if configured
        nlp_engine_name = config.get("nlp.engine")
        nlp_models = config.get("nlp.models")
        
        analyzer_params = {}
        if nlp_engine_name and nlp_models:
            try:
                nlp_config = {
                    "nlp_engine_name": nlp_engine_name,
                    "models": nlp_models,
                }
                nlp_engine = NlpEngineProvider(nlp_configuration=nlp_config).create_engine()
                analyzer_params["nlp_engine"] = nlp_engine
            except Exception as e:
                logger.error(f"Failed to initialize NLP engine '{nlp_engine_name}': {e}. Falling back to default.")

        # NIK Pattern (Indonesian Identity Number)
        nik_regex = r"\b[\dX]{16}\b"
        nik_pattern = Pattern(name="nik_pattern", regex=nik_regex, score=0.5)
        nik_recognizer = PatternRecognizer(
            supported_entity="ID_NIK", 
            patterns=[nik_pattern],
            context=["nik", "ktp", "penduduk"]
        )

        premasked_cc_no_regex = r"\b[\dX]{4}-[\dX]{4}-[\dX]{4}-[\dX]{4}\b"
        premasked_cc_no_pattern = Pattern(
            name="premasked_cc_no_pattern", 
            regex=premasked_cc_no_regex, 
            score=0.95
        )
        premasked_cc_no_recognizer = PatternRecognizer(
            supported_entity="PREMASKED_CC_NO", 
            patterns=[premasked_cc_no_pattern]
        )

        custom_deny_list = config.get("pii.custom_deny_list", [])
        custom_deny_list_recognizer = PatternRecognizer(
            supported_entity="CUSTOM_DENY_LIST", 
            deny_list=custom_deny_list or []
        )

        try:
            analyzer = AnalyzerEngine(**analyzer_params)
        except Exception as e:
            logger.error(f"Failed to initialize AnalyzerEngine: {e}")
            analyzer = AnalyzerEngine() # Last resort fallback

        analyzer.registry.add_recognizer(nik_recognizer)
        analyzer.registry.add_recognizer(premasked_cc_no_recognizer)
        analyzer.registry.add_recognizer(custom_deny_list_recognizer)

        gliner_model = config.get("nlp.gliner_model")
        if gliner_model:
            try:
                gliner_recognizer = GLiNERRecognizer(
                    model_name=gliner_model,
                    entity_mapping=_load_entity_mapping(),
                    flat_ner=False,
                    multi_label=True,
                    map_location="cpu"
                )
                analyzer.registry.add_recognizer(gliner_recognizer)
            except Exception as e:
                logger.error(f"Failed to initialize GLiNER recognizer with model {gliner_model}: {e}")

        try:
            analyzer.registry.remove_recognizer("SpacyRecognizer")
        except Exception:
            pass

        target_entities = config.get("pii.target_entities", [])
        if not target_entities:
            logger.warning("No target entities configured for PII anonymization.")
            target_entities = []
            
        for entity in ["CUSTOM_DENY_LIST", "PREMASKED_CC_NO", "ID_NIK"]:
            if entity not in target_entities:
                target_entities.append(entity)

        try:
            results = analyzer.analyze(
                text=text,
                entities=target_entities,
                language=config.get("nlp.analyzer_lang", "en")
            )
        except Exception as e:
            logger.error(f"Error during PII analysis: {e}")
            return

        operators = {
            entity: OperatorConfig("replace", {"new_value": "REDACTED"}) 
            for entity in target_entities
        }

        try:
            anonymizer = AnonymizerEngine()
            anonymized_result = anonymizer.anonymize(
                text=text, 
                analyzer_results=results,
                operators=operators
            )
            anonymized_text = anonymized_result.text
        except Exception as e:
            logger.error(f"Error during PII anonymization: {e}")
            return

        output_path = str(path).replace(".md", "-anonymized.md")
        if output_path == str(path):
            output_path = f"{str(path)}-anonymized.md"

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(anonymized_text)
                logger.info(f"Successfully anonymized text to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write anonymized file to {output_path}: {e}")

    except Exception as e:
        logger.critical(f"Unexpected error in pii_anonymizer: {e}", exc_info=True)
