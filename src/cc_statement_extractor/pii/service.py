from pathlib import Path

from cc_statement_extractor.shared.logger import logger
from cc_statement_extractor.shared.config import Config

def run_pii(generated_md_path: Path, config: Config) -> bool:
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
