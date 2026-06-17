# Temporal Question-Answering Dataset for Evolving Repositories

This repository builds a dataset for evaluating how well LLMs understand code evolution. It takes Git repositories, selects release-oriented semantic snapshots, extracts code metadata, generates snapshot-grounded QA pairs, samples representative evaluation sets, runs model evaluations, and produces thesis-ready result tables and figures.

## Dependencies

Use Python 3.10 or newer. The pipeline also shells out to `git` for repository cloning, diffs, and context retrieval, so Git must be available on `PATH`.

Create a virtual environment and install the Python dependencies:

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

## Run the Pipeline

Prepare a `repos.txt` file with one repository URL per line, then run the main steps:

```bash
python -m pipeline.main --repo-list repos.txt
python -m qa.qa_main --repos repos_name
python scripts/sample_qa.py --repo repos_name --budget 500
python -m eval.eval_main --repo repo_name --qa-file data/qa/repo_name/sampled.jsonl --model model_name --batch-size 8
```

For a single repository, replace the first command with:

```bash
python -m pipeline.main --repo-url https://github.com/user/repo
```

For API evaluation, replace the last command with:

```bash
python -m eval.eval_main --repo repo_name --qa-file data/qa/repo_name/sampled.jsonl --api-key KEY --api-base-url URL --api-model model_name --max-tokens 1024 --batch-size 10 --api-workers 5 --api-temperature 0
```

## Outputs

- `data/slices/{repo}/`: selected repository slices and extracted code metadata.
- `data/qa/{repo}/`: generated QA pools plus `sampled.jsonl` for evaluation.
- `data/eval_results/{model}/`: model predictions and summary metrics.

The repository is configured to track generated evaluation outputs and sampled QA files:

- `data/eval_results/**`
- `data/qa/*/sampled*.jsonl`
- `data/qa/sampled*.jsonl`

Heavy intermediate artifacts such as cloned repositories, slices, caches, and full unsampled QA pools stay ignored by Git.

## Project Layout

```text
requirements.txt     Python runtime and development dependencies

pipeline/            repository processing and semantic slice extraction

qa/                  QA generation from precomputed slice artifacts

eval/                model evaluation over sampled QA files

data/                generated and tracked experiment artifacts
  repositories/      cloned upstream repositories used for slice/context retrieval
  slices/{repo}/     selected slices and extracted metadata
  qa/{repo}/         generated QA pools and sampled evaluation files
  eval_results/      model predictions and summary metrics
```

## License

This project is part of a Master's thesis research project.
