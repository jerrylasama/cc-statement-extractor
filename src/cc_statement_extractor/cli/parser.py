import argparse

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract and anonymize CC statements.")
    parser.add_argument("-f", "--file_name", type=str, help="Process only this file (example: 2401.pdf or 2401)")
    parser.add_argument("--force", action="store_true", help="Force processing even if anonymized file exists")
    parser.add_argument("-d", "--dry-run", action="store_true", help="Simulate execution without moving data or extracting")
    return parser.parse_args()
