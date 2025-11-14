# python pipeline/02_export_snapshot.py --repo_path /path/to/repo --commit <hash> --out_dir ./data/snapshots/<hash>
import argparse
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

DEFAULT_EXCLUDES = ["tests", "docs"]


def _run_git_archive(repo_path: str, commit_hash: str, out_dir: Path, exclude: Iterable[str]):
    out_dir.mkdir(parents=True, exist_ok=True)
    archive_cmd: List[str] = [
        "git",
        "-C",
        repo_path,
        "archive",
        "--format=tar",
        "--prefix=repo/",
        commit_hash,
    ]
    pathspecs = [f":(exclude){pattern.strip()}" for pattern in exclude if pattern.strip()]
    if pathspecs:
        archive_cmd.append("--")
        archive_cmd.extend(pathspecs)

    proc = subprocess.Popen(archive_cmd, stdout=subprocess.PIPE)
    try:
        if not proc.stdout:
            raise RuntimeError("Failed to stream archive output")
        with tarfile.open(fileobj=proc.stdout, mode="r|*") as tar:
            tar.extractall(path=out_dir)
    finally:
        proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"git archive failed with return code {proc.returncode}")

    extracted_root = out_dir / "repo"
    source_dir = out_dir / "source"
    if source_dir.exists():
        shutil.rmtree(source_dir)
    if extracted_root.exists():
        extracted_root.rename(source_dir)
    else:
        raise FileNotFoundError("Expected archive root 'repo/' was not created")


def _run_git_worktree(repo_path: str, commit_hash: str, source_dir: Path, exclude: Iterable[str]):
    temp_dir = Path(tempfile.mkdtemp(prefix="snapshot-worktree-"))
    try:
        subprocess.run(
            ["git", "-C", repo_path, "worktree", "add", "--detach", str(temp_dir), commit_hash],
            check=True,
        )
        ignore_patterns = [".git"] + list(exclude)
        if source_dir.exists():
            shutil.rmtree(source_dir)
        shutil.copytree(temp_dir, source_dir, ignore=shutil.ignore_patterns(*ignore_patterns))
    finally:
        subprocess.run(
            ["git", "-C", repo_path, "worktree", "remove", "-f", str(temp_dir)],
            check=False,
        )
        shutil.rmtree(temp_dir, ignore_errors=True)


def checkout_and_export(
    repo_path: str,
    commit_hash: str,
    out_dir: str,
    exclude: Optional[Iterable[str]] = None,
    metadata: Optional[Dict] = None,
):
    """Export a repository snapshot for the given commit into out_dir/source."""
    repo_path = str(Path(repo_path).expanduser().resolve())
    out_path = Path(out_dir)
    if out_path.exists():
        shutil.rmtree(out_path)
    out_path.mkdir(parents=True, exist_ok=True)
    source_dir = out_path / "source"
    excludes = list(DEFAULT_EXCLUDES)
    if exclude:
        excludes.extend(exclude)

    try:
        _run_git_archive(repo_path, commit_hash, out_path, excludes)
    except Exception:
        _run_git_worktree(repo_path, commit_hash, source_dir, excludes)

    snapshot_meta = dict(metadata or {})
    snapshot_meta.update(
        {
            "commit": commit_hash,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "source_dir": str(source_dir),
        }
    )
    with open(out_path / "metadata.json", "w", encoding="utf-8") as f:
        json.dump(snapshot_meta, f, indent=2, ensure_ascii=False)
    print(f"Exported snapshot for {commit_hash} to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export a repository snapshot for a commit.")
    parser.add_argument("--repo_path", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--out_dir", required=True)
    parser.add_argument(
        "--exclude",
        nargs="*",
        default=[],
        help="Optional directory patterns to exclude (in addition to defaults)",
    )
    args = parser.parse_args()
    checkout_and_export(args.repo_path, args.commit, args.out_dir, exclude=args.exclude)
