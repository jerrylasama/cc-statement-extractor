import pytest
from unittest.mock import patch
import sys

from cc_statement_extractor.cli.parser import parse_args


def test_parse_args_default():
    with patch.object(sys, "argv", ["cc-statement"]):
        args = parse_args()
        assert getattr(args, "file_name", None) is None
        assert getattr(args, "force", False) is False
        assert getattr(args, "dry_run", False) is False


def test_parse_args_with_file_name():
    with patch.object(sys, "argv", ["cc-statement", "-f", "test.pdf"]):
        args = parse_args()
        assert args.file_name == "test.pdf"
        assert not args.force
        assert not args.dry_run

    with patch.object(sys, "argv", ["cc-statement", "--file_name", "test2.pdf"]):
        args = parse_args()
        assert args.file_name == "test2.pdf"


def test_parse_args_with_options():
    with patch.object(sys, "argv", ["cc-statement", "--force", "-d"]):
        args = parse_args()
        assert args.file_name is None
        assert args.force is True
        assert args.dry_run is True


def test_parse_args_unrecognized():
    with patch.object(sys, "argv", ["cc-statement", "--unknown-flag"]):
        with pytest.raises(SystemExit):
            parse_args()
