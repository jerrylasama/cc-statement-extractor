from unittest.mock import patch, MagicMock

from cc_statement_extractor.main import main


@patch("cc_statement_extractor.main.logger")
@patch("cc_statement_extractor.main.run_pipeline")
@patch("cc_statement_extractor.main.parse_args")
def test_main_success(mock_parse_args, mock_run_pipeline, mock_logger):
    mock_args = MagicMock()
    mock_parse_args.return_value = mock_args

    main()

    mock_parse_args.assert_called_once()
    mock_run_pipeline.assert_called_once_with(mock_args)
    mock_logger.critical.assert_not_called()


@patch("cc_statement_extractor.main.logger")
@patch("cc_statement_extractor.main.run_pipeline")
@patch("cc_statement_extractor.main.parse_args")
def test_main_critical_error(mock_parse_args, mock_run_pipeline, mock_logger):
    mock_parse_args.side_effect = Exception("Test unexpected error")

    main()

    mock_logger.critical.assert_called_once()
    args, kwargs = mock_logger.critical.call_args
    assert "A critical error occurred in the extraction pipeline" in args[0]
    assert "Test unexpected error" in args[0]
    assert kwargs.get("exc_info") is True
