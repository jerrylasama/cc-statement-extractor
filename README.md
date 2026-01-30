# Credit Card Statement Extractor

Small tool to extract and anonymize data from credit card statements (PDFs), since my bank doesn't provide a good way to export the data and I am too lazy to do it manually. It uses **PaddleOCR** for the OCR and **Microsoft Presidio** for PII (Personally Identifiable Information) anonymization.

## Features

- **OCR Extraction**: Converts PDF statements into structured Markdown, ready to be used by other tools / LLMs.
- **PII Anonymization**: Automatically redacts sensitive information like:
    - Names
    - Credit Card Numbers
    - Identity Numbers (NIK)
    - Locations
    - Email Addresses
    - Anything else you want to redact, just edit `config.yaml`
- **Custom Deny List**: Add specific terms to be redacted in `config.yaml`.
- **Incremental Processing**: Skips already processed files unless forced.

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
# Install dependencies (CPU version)
uv sync

# If you have a CUDA Acceleration and want faster OCR
# Though, don't forget to match your CUDA version with the one in pyproject.toml's "paddle-gpu" extra
# More Info: https://www.paddlepaddle.org.cn/install/quick?docurl=/documentation/docs/zh/develop/install/pip/linux-pip.html
uv sync --extra gpu
```

Install Spacy model

```bash
uv run -m spacy download en_core_web_sm
```

## Usage

Place your PDF statements in the `data/` directory.

```bash
# Process all PDFs in data/
uv run main

# Process a specific file
uv run main -f filename.pdf

# Force re-processing of already anonymized files
uv run main --force
```

Results will be saved in the `output/` directory as `-anonymized.md` files.

## Configuration

Configuration is managed via `config.yaml`. Copy `config.example.yaml` to create your own:

```bash
cp config.example.yaml config.yaml
```

Configuration areas:
- `ocr`: Control which labels to ignore during Markdown generation.
- `nlp`: Configure the NLP engine (spaCy) and GLiNER models (GLiNER).
- `pii`: Define target entities and a custom deny list for redaction.

GLiNER entity mapping is done on entity.map.json, if you use any other model, don't forget to adjust it.

## Docker Usage

You can run the extractor using Docker to avoid local environment issues.

### Using CPU

```bash
# Build
docker build -t cc-extractor-cpu .

# Run all files
docker run -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output -v $(pwd)/config.yaml:/app/config.yaml cc-extractor-cpu

# Pass parameters (e.g., process specific file)
docker run -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output -v $(pwd)/config.yaml:/app/config.yaml cc-extractor-cpu -f filename.pdf --force
```

### Using GPU (CUDA 13.0)

Requires [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html).

```bash
# Build
docker build -t cc-extractor-gpu -f Dockerfile.gpu .

# Run all files
docker run --gpus all -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output -v $(pwd)/config.yaml:/app/config.yaml cc-extractor-gpu

# Pass parameters
docker run --gpus all -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output -v $(pwd)/config.yaml:/app/config.yaml cc-extractor-gpu --force
```

## Development

```bash
# Run tests
uv run pytest

# Check linting
uv run ruff check
```

## License

MIT License - See the [LICENSE](LICENSE) file for details.
