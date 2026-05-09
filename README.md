# Temporal Question-Answering Dataset for Evolving Repositories

A pipeline for extracting semantic evolution slices from Git repositories and generating snapshot-grounded question-answer pairs for evaluating LLMs' understanding of code evolution.

## Overview

This project implements an automated pipeline that:
1. Clones and analyzes Git repositories
2. Identifies semantic evolution slices with a tag-distance + dynamic programming strategy
3. Extracts ASTs, function signatures, and metadata for each slice
4. Generates slice-grounded Q&A pairs for temporal code understanding evaluation

## Features

- **Semantic Slicing**: Uses all release-like tags as anchors, computes adjacent-tag semantic distance, and selects slices under budget with DP
- **Multi-language Support**: Currently supports Python and Java
- **AST Parsing**: Extracts structured code information using tree-sitter, including rich metadata for QA generation (function parameters, return types, decorators, class fields, and imports)
- **Validation**: Ensures slice quality and code parseability
- **Flexible Configuration**: Customizable slicing and parsing settings via YAML config

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
- `--existing-repo-action`: Action when local repo already exists: `ask` / `update` / `skip` (if omitted, uses `storage.existing_repo_action` from config)

### Generate QA Pairs From Slices

After the slicing pipeline has produced `data/slices/{repo_name}/...`, you can generate QA pairs from those precomputed slice artifacts with:

```bash
python -m qa.qa_main --repos repos_name
```

Optional arguments:

- `--slices-root`: Root directory that contains repository slices (default: `data/slices`)
- `--output-dir`: Directory for generated QA data (default: `data/qa`)
- `--repos`: Comma-separated repository names to process, for example `colorama,fastapi`

The QA generator reads each repository's `slices/slice_XXXX/symbols/` data, then writes the following files under `data/qa/{repo_name}/`:

- `intrinsic_qa_pairs.jsonl`: symbol- and structure-based QA pairs
- `extrinsic_qa_pairs.jsonl`: docstring-derived QA pairs
- `temporal_qa_pairs.jsonl`: cross-slice evolution QA pairs
- `qa_pairs.jsonl`: combined output of all QA pairs
- `summary.json`: per-repository QA statistics

The top-level `data/qa/summary.json` aggregates totals across all processed repositories.

### Sample High-Quality QA Pairs

After generating QA pairs, use the stratified sampler to select a fixed number of high-quality, representative pairs from one or more repositories:

```bash
# Single repository — outputs data/qa/fastapi/sampled.jsonl
python scripts/sample_qa.py --repo fastapi --budget 5000

# Multiple repositories — each gets its own sampled.jsonl, plus a merged file
python scripts/sample_qa.py --repo fastapi spring-boot kafka keras transformers --budget 5000

# Custom output path and budget
python scripts/sample_qa.py --repo spring-boot --budget 3000 --output data/qa/spring-boot/sampled_3k.jsonl
```

The sampler applies three quality filters before drawing samples:

1. **Binary questions balanced** — for Yes/No subtypes (e.g. `symbol_existence`), Yes and No answers are sampled 1:1 to avoid label skew.
2. **Per-file cap** — at most 8 QA pairs per source file, preventing tutorial-heavy repos from dominating.

The type-level budget split and per-subtype weights are fully configurable at the top of `scripts/sample_qa.py` (`TYPE_ALLOC` and `SUBTYPE_WEIGHTS`). Default allocation:

| Type | Share | Rationale |
|------|-------|-----------|
| intrinsic | 55 % | Largest and most diverse pool |
| temporal | 35 % | High-value evolution questions |
| extrinsic | 10 % | Scarce but high-quality docstring questions |

Any shortfall in one type (e.g. too few extrinsic QAs) is automatically redistributed to the remaining types so the total always reaches the requested `--budget`.

### Evaluate LLM on QA Pairs

After sampling, run the evaluator against a local HuggingFace model:

```bash
python -m eval.eval_main \
    --repo fastapi \
    --qa-type all \
    --model Qwen/Qwen2.5-Coder-7B-Instruct \
    --batch-size 8 \
    --max-tokens 256
```

Or evaluate against a pre-sampled file directly:

```bash
python -m eval.eval_main \
    --repo fastapi \
    --qa-file data/qa/fastapi/sampled.jsonl \
    --model Qwen/Qwen2.5-Coder-7B-Instruct \
    --batch-size 8
```

Key arguments:

| Argument | Default | Description |
|---|---|---|
| `--repo` | *(required)* | Repository name matching `data/qa/{repo}/` |
| `--qa-type` | `intrinsic` | `intrinsic` / `extrinsic` / `temporal` / `all` |
| `--qa-file` | — | Direct path to a QA JSONL file (overrides `--qa-type`) |
| `--model` | `Qwen/Qwen2.5-Coder-7B-Instruct` | HuggingFace model ID |
| `--device-map` | `auto` | Passed to the HuggingFace pipeline |
| `--batch-size` | `8` | Prompts per inference batch (increase for larger VRAM) |
| `--max-tokens` | `256` | Max new tokens per answer |
| `--sample` | — | Randomly sample N pairs before evaluation |
| `--subtype` | — | Restrict to a specific `qa_subtype` |
| `--output-dir` | `data/eval_results` | Directory for result JSONL files |

Results are written to `data/eval_results/{repo}_{tag}_{timestamp}.jsonl`. Each line contains the question, ground truth, model prediction, and per-item metrics (Exact Match, Token F1, ROUGE-L). An aggregate summary is printed to stdout at the end.

## Configuration

Edit `config.yaml` to customize:

- **Slicing parameters**: Target slices, distance weights, and DP gain function
- **Language support**: Configure which languages to parse and their file extensions
- **Storage paths**: Set output, cache, and repository directories
- **Repository selection**: Criteria for filtering repositories (commit counts, licenses, etc.)
- **Validation settings**: Quality thresholds and build checking options

Example configuration:
```yaml
slicing:
  target_slices: 20
  distance_weights:
    lines: 0.45
    files: 0.45
    api_break: 0.10
  segment_gain: "log1p"
  force_first_release_tag: true
  min_days_between_selected: 0

parsing:
  languages: ["python", "java"]
  timeout_seconds: 30

storage:
  output_dir: "./data/slices"
  cache_dir: "./data/cache"
  repositories_dir: "./data/repositories"
  existing_repo_action: "ask"   # ask | update | skip
```

`existing_repo_action` behavior:
- `ask`: Prompt whether to update existing local repository
- `update`: Run `git pull` and continue
- `skip`: Reuse local repository without pulling

Note: In non-interactive environments, `ask` automatically falls back to `skip`.

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
├── eval/                  # LLM evaluation
│   ├── eval_main.py          # CLI entry point
│   ├── llm_client.py         # HuggingFace inference client (batched)
│   ├── evaluator.py          # Batch evaluation loop
│   ├── context_retriever.py  # Fetch source at commit via git
│   ├── prompt_builder.py     # Per-type/subtype prompt templates
│   └── metrics.py            # Exact Match, Token F1, ROUGE-L
├── scripts/               # Utility scripts
│   ├── sample_qa.py          # Stratified QA sampler
│   └── distance_ablation.py  # Slice distance ablation
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
    │   └── symbols/           # QA-enriched symbol data
    │       ├── functions.json # Typed params, decorators, documentation
    │       ├── classes.json   # Fields, method lists, documentation
    │       ├── imports.json   # Import statements
    │       └── module_docs.json # Module-level docstrings
    └── slice_0002/
        └── ...
```

### Key Files

- **`metadata.json`**: Contains repository information and a list of all slices with their metadata
- **`summary.json`**: Overall statistics including total slices, files, lines, functions, classes, and language distribution
- **`slices/slice_XXXX/metadata.json`**: Individual slice metadata (commit hash, date, type, version tag, etc.)
- **`slices/slice_XXXX/files.json`**: List of files in the slice with their paths, content hashes, and languages
- **`slices/slice_XXXX/symbols/`**: QA-enriched symbol data (functions, classes, imports, module docstrings) from all files in the slice


## License

This project is part of a Master's thesis research project.
