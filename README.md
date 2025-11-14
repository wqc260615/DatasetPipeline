# Dataset Pipeline (Transformers Temporal Benchmark)

This repository builds evolution-aware datasets from git repositories (e.g., `huggingface/transformers`). The pipeline covers:
- `01_index_repo.py`: traverse commits via PyDriller, record parents, timestamps, tag metadata.
- `02_export_snapshot.py`: export a clean snapshot per commit (git archive → worktree fallback) with optional excludes.
- `03_parse_snapshot.py`: AST pass for Python files (functions/classes/imports/calls, LOC).
- `04_diff_snapshot.py`: git diff stats and patch capture against the parent commit.
- `05_build_metadata.py`: aggregate snapshot, parse, and diff outputs into a uniform `metadata.json`.
- `slicer.py`: commit selection strategies (`commit`, `tag`, `release`, `time-interval`).
- `run_pipeline.py`: orchestrates the full flow with logging, tqdm progress, and error isolation.

## Quick Start
```bash
python pipeline/run_pipeline.py \
  --repo_url https://github.com/huggingface/transformers \
  --output_dir ./dataset \
  --slice_mode release \
  --limit_commits 100 \
  --index_limit 5000 \
  --time_interval 30d   # only used for time-interval slicing
```

Outputs follow:
```
dataset/<repo_name>/
  index.json
  pipeline.log
  summary.json
  snapshots/<commit>/
    source/...
    parsed.json
    diff.json
    metadata.json
```

### Sample `metadata.json`
```json
{
  "commit": "0e4b7938d0e965362973797f47ad2b85f605a96a",
  "timestamp": "2025-07-15T08:40:41+00:00",
  "message": "Add ModernBERT Decoder Models - ModernBERT, but trained with CLM! (#38967)",
  "parent": "0b724114cf8475f146ca2fd644c4e31f395441eb",
  "num_files": 2803,
  "loc": 1056661,
  "languages": [
    "other",
    "python"
  ],
  "stats": {
    "files_changed": [
      {
        "path": "docs/source/en/_toctree.yml",
        "lines_added": 2,
        "lines_deleted": 0
      },
      {
        "path": "docs/source/en/model_doc/modernbert-decoder.md",
        "lines_added": 155,
        "lines_deleted": 0
      }
      ...
    ],
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

If a repository already exists locally, pass its path to `--repo_url` (the script detects local paths). Errors per commit are logged but do not stop the run; inspect `summary.json` or `pipeline.log` for details.

