from cc_statement_extractor.cli.parser import parse_args
from cc_statement_extractor.pipeline.orchestrator import run_pipeline
from cc_statement_extractor.shared.logger import logger

def main() -> None:
    """
    Main entry point for the credit card statement extraction pipeline.
    This function delegates CLI argument parsing and pipeline orchestration.
    """
    try:
        args = parse_args()
        run_pipeline(args)
    except Exception as e:
        logger.critical(f"A critical error occurred in the extraction pipeline: {e}", exc_info=True)

if __name__ == "__main__":
    main()