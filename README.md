# Dataset Pipeline (Temporal Q&A Benchmark for Evolving Repositories)

This repository builds evolution-aware datasets from git repositories for evaluating LLMs' temporal reasoning capabilities. The pipeline implements the data collection and Q&A generation components described in the thesis proposal.

## Pipeline Overview

The pipeline consists of three phases:

### Phase 1: Snapshot Extraction
| Module | Description |
|--------|-------------|
| `01_index_repo.py` | Traverse commits via PyDriller, record parents, timestamps, tag metadata |
| `02_export_snapshot.py` | Export clean snapshot per commit (git archive → worktree fallback) |
| `03_parse_snapshot.py` | AST pass for Python files (functions/classes/imports/calls, LOC) |
| `04_diff_snapshot.py` | Git diff stats and patch capture against parent commit |
| `05_build_metadata.py` | Aggregate snapshot, parse, and diff outputs into `metadata.json` |

### Phase 2: Cross-Version Analysis
| Module | Description |
|--------|-------------|
| `06_track_entities.py` | Track entity lifecycle (introduction, modification, removal) across versions |
| `07_detect_changes.py` | Detect code evolution events (rename, move, signature change, refactor) |

### Phase 3: Q&A Generation
| Module | Description |
|--------|-------------|
| `08_generate_qa.py` | Generate temporally grounded Q&A pairs for RQ1-RQ4 evaluation |

### Utilities
| Module | Description |
|--------|-------------|
| `slicer.py` | Commit selection strategies (`commit`, `tag`, `release`, `time-interval`) |
| `run_pipeline.py` | Orchestrates the full flow with logging, progress bars, and error isolation |

## Research Questions Supported

The generated Q&A dataset supports evaluation of the following research questions:

- **RQ1**: Factual questions about repository state at specific versions
  - Entity existence ("Does function X exist in v1.0?")
  - Introduction timing ("When was class Y introduced?")
  - Signature queries ("What are the parameters of function Z?")

- **RQ2**: Sensitivity to code evolution events
  - Rename detection ("Was function A renamed to B?")
  - Move detection ("Was class C moved to a different file?")
  - Signature changes ("Did the API of function D change?")

- **RQ3**: Temporal leakage detection
  - Questions about entities before their introduction
  - Questions about entities after their removal

- **RQ4**: Multi-version reasoning
  - Evolution summaries ("How has function X evolved?")
  - Backward compatibility analysis

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
python pipeline/run_pipeline.py \
  --repo_url https://github.com/huggingface/transformers \
  --output_dir ./dataset \
  --slice_mode release \
  --limit_commits 100 \
  --index_limit 5000
```

### Slicing Modes

| Mode | Description |
|------|-------------|
| `commit` | Process all commits (up to limit) |
| `tag` | Process only commits with tags |
| `release` | Process only commits with version-like tags (v1.0, etc.) |
| `time-interval` | Process commits at regular intervals (e.g., `--time_interval 30d`) |

## Output Structure

```
dataset/<repo_name>/
├── index.json              # Commit index with metadata
├── pipeline.log            # Execution log
├── summary.json            # Pipeline execution summary
├── entity_timeline.json    # Entity lifecycle tracking (RQ1)
├── change_history.json     # Code evolution events (RQ2)
├── qa_dataset.json         # Generated Q&A pairs (RQ1-RQ4)
└── snapshots/
    └── <commit_hash>/
        ├── source/         # Extracted source code
        ├── parsed.json     # AST analysis results
        ├── diff.json       # Diff from parent commit
        └── metadata.json   # Aggregated metadata
```

## Sample Outputs

### `entity_timeline.json` (Entity Lifecycle)
```json
{
  "summary": {
    "total_entities_tracked": 1250,
    "functions_tracked": 980,
    "classes_tracked": 270
  },
  "entity_index": {
    "function::src/model.py::forward": {
      "introduced_in": "abc123",
      "introduced_at": "2024-01-15T10:30:00Z",
      "modifications": [...],
      "removed_in": null
    }
  }
}
```

### `change_history.json` (Evolution Events)
```json
{
  "summary": {
    "total_version_pairs": 45,
    "global_change_counts": {
      "added": 120,
      "removed": 30,
      "renamed": 15,
      "signature_changed": 45
    }
  }
}
```

### `qa_dataset.json` (Q&A Pairs)
```json
{
  "summary": {
    "total_questions": 500,
    "questions_by_rq": {
      "RQ1": 200,
      "RQ2": 150,
      "RQ3": 80,
      "RQ4": 70
    }
  },
  "questions": [
    {
      "id": "q_00001",
      "rq": "RQ1",
      "category": "existence",
      "question": "Does the function `forward` exist in version v2.0?",
      "answer": "Yes",
      "ground_truth": true,
      "commit": "abc123def",
      "metadata": {
        "entity_type": "function",
        "question_type": "boolean"
      }
    }
  ]
}
```

### `metadata.json` (Per-Snapshot)
```json
{
  "commit": "0e4b7938d0e965362973797f47ad2b85f605a96a",
  "timestamp": "2025-07-15T08:40:41+00:00",
  "message": "Add ModernBERT Decoder Models",
  "parent": "0b724114cf8475f146ca2fd644c4e31f395441eb",
  "num_files": 2803,
  "loc": 1056661,
  "languages": ["python", "other"],
  "stats": {
    "files_changed": [...],
    "lines_added": 2020,
    "lines_deleted": 0
  },
  "ast_summary": {
    "files": 2521,
    "functions": 36380,
    "classes": 10836,
    "imports": 38723,
    "calls": 265279,
    "loc": 1056661
  }
}
```

## Running Individual Modules

Each module can be run independently:

```bash
# Index repository
python pipeline/01_index_repo.py --repo_path /path/to/repo --out ./data/index.json

# Export snapshot
python pipeline/02_export_snapshot.py --repo_path /path/to/repo --commit abc123 --out_dir ./data/snapshots/abc123

# Parse snapshot
python pipeline/03_parse_snapshot.py --snapshot_dir ./data/snapshots/abc123 --out ./data/snapshots/abc123/parsed.json

# Build entity timeline
python pipeline/06_track_entities.py --snapshots_dir ./data/snapshots --index_path ./data/index.json --out ./data/entity_timeline.json

# Detect changes
python pipeline/07_detect_changes.py --snapshots_dir ./data/snapshots --index_path ./data/index.json --out ./data/change_history.json

# Generate Q&A
python pipeline/08_generate_qa.py \
  --entity_timeline ./data/entity_timeline.json \
  --change_history ./data/change_history.json \
  --index_path ./data/index.json \
  --snapshots_dir ./data/snapshots \
  --out ./data/qa_dataset.json
```

## Error Handling

Errors per commit are logged but do not stop the run. Inspect `summary.json` or `pipeline.log` for details. If a repository already exists locally, pass its path to `--repo_url` (the script detects local paths).

## License

MIT
