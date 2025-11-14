"""End-to-end dataset construction pipeline."""

import argparse
import importlib.util
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

from tqdm import tqdm

try:
    from . import slicer  # type: ignore
except ImportError:
    import slicer  # type: ignore


def _load_module(script_name: str, alias: str):
    script_path = Path(__file__).with_name(script_name)
    spec = importlib.util.spec_from_file_location(alias, script_path)
    module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise ImportError(f"Cannot load module for {script_name}")
    spec.loader.exec_module(module)
    return module


INDEX_MODULE = _load_module("01_index_repo.py", "pipeline_index")
EXPORT_MODULE = _load_module("02_export_snapshot.py", "pipeline_export")
PARSE_MODULE = _load_module("03_parse_snapshot.py", "pipeline_parse")
DIFF_MODULE = _load_module("04_diff_snapshot.py", "pipeline_diff")
METADATA_MODULE = _load_module("05_build_metadata.py", "pipeline_metadata")


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("dataset_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def repo_name_from_url(repo_url: str) -> str:
    path = repo_url.rstrip("/").split("/")[-1]
    return path[:-4] if path.endswith(".git") else path


def ensure_repo(repo_url: str, cache_dir: Path, logger: logging.Logger) -> Path:
    candidate = Path(repo_url).expanduser()
    if candidate.exists():
        logger.info("Using existing local repository at %s", candidate)
        return candidate.resolve()

    repo_dir = cache_dir / ".repo"
    if repo_dir.exists():
        logger.info("Updating existing clone in %s", repo_dir)
        subprocess.run(["git", "-C", str(repo_dir), "fetch", "--all", "--tags", "--prune"], check=True)
    else:
        logger.info("Cloning %s into %s", repo_url, repo_dir)
        repo_dir.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", repo_url, str(repo_dir)], check=True)
    return repo_dir.resolve()


def write_index(commits: List[Dict], index_path: Path, repo_path: Path):
    payload = {
        "repo_path": str(repo_path),
        "commit_count": len(commits),
        "commits": commits,
    }
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def process_commit(
    commit: Dict,
    repo_path: Path,
    snapshot_dir: Path,
    logger: logging.Logger,
) -> bool:
    commit_hash = commit["hash"]
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    parsed_path = snapshot_dir / "parsed.json"
    diff_path = snapshot_dir / "diff.json"
    metadata_path = snapshot_dir / "metadata.json"

    export_metadata = {
        "commit": commit_hash,
        "timestamp": commit.get("timestamp"),
        "message": commit.get("message"),
        "author": commit.get("author"),
        "author_email": commit.get("author_email"),
        "parent": commit.get("parent"),
        "tags": commit.get("tags", []),
    }

    EXPORT_MODULE.checkout_and_export(str(repo_path), commit_hash, str(snapshot_dir), metadata=export_metadata)

    parsed = PARSE_MODULE.parse_snapshot(str(snapshot_dir))
    with open(parsed_path, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2, ensure_ascii=False)

    diff_data = DIFF_MODULE.build_diff(str(repo_path), commit_hash)
    with open(diff_path, "w", encoding="utf-8") as f:
        json.dump(diff_data, f, indent=2, ensure_ascii=False)

    METADATA_MODULE.build_metadata(str(snapshot_dir), str(metadata_path))
    logger.info("Finished commit %s", commit_hash)
    return True


def run_pipeline(
    repo_url: str,
    output_dir: str,
    slice_mode: str,
    limit_commits: Optional[int],
    time_interval: Optional[str],
    index_limit: Optional[int],
) -> Dict:
    output_path = Path(output_dir).expanduser().resolve()
    repo_name = repo_name_from_url(repo_url)
    repo_output_dir = output_path / repo_name
    repo_output_dir.mkdir(parents=True, exist_ok=True)

    log_path = repo_output_dir / "pipeline.log"
    logger = setup_logging(log_path)
    logger.info("Starting pipeline for %s", repo_url)

    repo_path = ensure_repo(repo_url, repo_output_dir, logger)

    commits = INDEX_MODULE.index_repo(str(repo_path), limit_commits=index_limit)
    index_path = repo_output_dir / "index.json"
    write_index(commits, index_path, repo_path)
    logger.info("Indexed %d commits", len(commits))

    sliced = slicer.slice_commits(
        commits,
        mode=slice_mode,
        interval=time_interval,
        limit=limit_commits,
    )
    logger.info("Selected %d commits using mode=%s", len(sliced), slice_mode)

    snapshot_root = repo_output_dir / "snapshots"
    failures = []
    for commit in tqdm(sliced, desc="Processing commits"):
        commit_hash = commit["hash"]
        try:
            process_commit(commit, repo_path, snapshot_root / commit_hash, logger)
        except Exception as exc:
            logger.exception("Failed to process %s: %s", commit_hash, exc)
            failures.append({"commit": commit_hash, "error": str(exc)})

    summary = {
        "repo": repo_url,
        "output_dir": str(repo_output_dir),
        "total_commits": len(commits),
        "processed": len(sliced) - len(failures),
        "failed": failures,
    }
    summary_path = repo_output_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    logger.info("Pipeline completed with %d successes and %d failures", summary["processed"], len(failures))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the dataset construction pipeline end-to-end.")
    parser.add_argument("--repo_url", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument(
        "--slice_mode",
        choices=["commit", "tag", "release", "time-interval"],
        default="commit",
    )
    parser.add_argument(
        "--limit_commits",
        type=int,
        default=None,
        help="Limit number of commits to process after slicing",
    )
    parser.add_argument(
        "--time_interval",
        default=None,
        help="Interval string for time-interval mode (e.g. 30d)",
    )
    parser.add_argument(
        "--index_limit",
        type=int,
        default=None,
        help="Limit number of commits recorded during indexing",
    )
    args = parser.parse_args()

    run_pipeline(
        repo_url=args.repo_url,
        output_dir=args.output_dir,
        slice_mode=args.slice_mode,
        limit_commits=args.limit_commits,
        time_interval=args.time_interval,
        index_limit=args.index_limit,
    )

