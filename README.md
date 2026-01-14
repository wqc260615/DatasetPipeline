# Temporal Question-Answering Dataset for Evolving Repositories

A pipeline for extracting semantic evolution slices from Git repositories and generating snapshot-grounded question-answer pairs for evaluating LLMs' understanding of code evolution.

## Overview

This project implements an automated pipeline that:
1. Clones and analyzes Git repositories
2. Identifies semantic evolution slices based on version releases, feature integrations, API changes, and refactoring events
3. Extracts ASTs, function signatures, and metadata for each slice
4. Generates slice-grounded Q&A pairs for temporal code understanding evaluation

## Features

- **Semantic Slicing**: Automatically identifies meaningful code evolution points from Git history
- **Multi-language Support**: Currently supports Python and Java
- **AST Parsing**: Extracts structured code information using tree-sitter
- **Validation**: Ensures slice quality and code parseability
- **Flexible Configuration**: Customizable thresholds and settings via YAML config

## Installation

### Prerequisites

- Python 3.10 or higher
- Git

### Setup

1. **Create virtual environment:**
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. **Install dependencies:**
```bash
pip install -r requirements.txt
```

The requirements include:
- GitPython for repository operations
- tree-sitter for AST parsing
- pydantic for data models
- pyyaml for configuration

## Usage

### Process a Single Repository

```bash
python -m pipeline.main --repo-url https://github.com/user/repo
```

### Process Multiple Repositories

Create a file `repos.txt` with one URL per line:
```
https://github.com/user/repo1
https://github.com/user/repo2
```

Then run:
```bash
python -m pipeline.main --repo-list repos.txt
```

### Command Line Options

- `--repo-url`: Single repository URL to process
- `--repo-list`: Path to file containing repository URLs (one per line)
- `--config`: Path to configuration file (default: `config.yaml`)
- `--output-dir`: Output directory for results (default: `./data/slices`)

## Configuration

Edit `config.yaml` to customize:

- **Slicing parameters**: Minimum interval between slices, maximum slices per repo, feature thresholds
- **Language support**: Configure which languages to parse and their file extensions
- **Storage paths**: Set output, cache, and repository directories
- **Repository selection**: Criteria for filtering repositories (commit counts, licenses, etc.)
- **Validation settings**: Quality thresholds and build checking options

Example configuration:
```yaml
slicing:
  min_interval_days: 14
  max_slices_per_repo: 15
  major_feature_threshold_lines: 200

parsing:
  languages: ["python", "java"]
  timeout_seconds: 30

storage:
  output_dir: "./data/slices"
  cache_dir: "./data/cache"
  repositories_dir: "./data/repositories"
```

## Project Structure

```
DatasetPipeline/
├── pipeline/              # Main pipeline code
│   ├── repository_cloner.py      # Git cloning operations
│   ├── commit_extractor.py       # Commit extraction and analysis
│   ├── semantic_slicer.py        # Slice identification algorithm
│   ├── ast_parser.py             # AST parsing with tree-sitter
│   ├── metadata_generator.py     # Metadata generation for slices
│   ├── models.py                 # Data models (Pydantic)
│   ├── config.py                 # Configuration loading
│   ├── output_writer.py          # Output file writing
│   ├── main.py                   # Entry point
│   └── validation/               # Validation modules
│       ├── slice_validator.py    # Slice quality validation
│       └── build_checker.py      # Build/compilation checking
├── tests/                 # Unit tests
├── config.yaml           # Configuration file
├── requirements.txt      # Python dependencies
├── pytest.ini           # Pytest configuration
└── README.md            # This file
```

## Output Format

The pipeline generates structured JSON files in the output directory (default: `./data/slices/`). Each repository gets its own subdirectory with the following structure:

```
data/slices/{repo_name}/
├── metadata.json              # Repository info + slice metadata list
├── summary.json               # Overall statistics
└── slices/
    ├── slice_0001/
    │   ├── metadata.json      # Individual slice metadata
    │   ├── files.json         # File list with content hashes
    │   └── symbols/           # Symbol-level data
    │       ├── functions.json # Extracted functions
    │       ├── classes.json   # Extracted classes
    │       └── comments.json  # Extracted comments
    └── slice_0002/
        └── ...
```

### Key Files

- **`metadata.json`**: Contains repository information and a list of all slices with their metadata
- **`summary.json`**: Overall statistics including total slices, files, lines, functions, classes, and language distribution
- **`slices/slice_XXXX/metadata.json`**: Individual slice metadata (commit hash, date, type, version tag, etc.)
- **`slices/slice_XXXX/files.json`**: List of files in the slice with their paths, content hashes, and languages
- **`slices/slice_XXXX/symbols/`**: Symbol-level extracted data (functions, classes, comments) from all files in the slice

## Testing

Run unit tests:
```bash
pytest tests/
```

Run with coverage:
```bash
pytest tests/ --cov=pipeline --cov-report=html
```


## License

This project is part of a Master's thesis research project.
