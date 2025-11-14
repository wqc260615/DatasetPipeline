"""Compute diff metadata for a commit."""

import argparse
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def _git(repo_path: str, *args: str, check: bool = True, capture_output: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", repo_path, *args],
        check=check,
        capture_output=capture_output,
        text=True,
    )


def _get_parent(repo_path: str, commit: str) -> Optional[str]:
    try:
        result = _git(repo_path, "rev-parse", f"{commit}^", check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def _collect_numstat(repo_path: str, parent: Optional[str], commit: str) -> Tuple[List[Dict], int, int]:
    if parent:
        args = ["diff", "--numstat", parent, commit]
    else:
        args = ["diff", "--numstat", "--root", commit]
    result = _git(repo_path, *args)
    files = []
    total_added = 0
    total_deleted = 0
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        added_str, deleted_str, path = line.split("\t")
        if added_str == "-":
            added = deleted = 0
        else:
            added = int(added_str)
            deleted = int(deleted_str)
        total_added += added
        total_deleted += deleted
        files.append({"path": path, "lines_added": added, "lines_deleted": deleted})
    return files, total_added, total_deleted


def _collect_patch(repo_path: str, parent: Optional[str], commit: str) -> str:
    if parent:
        args = ["diff", parent, commit]
    else:
        args = ["diff", "--root", commit]
    result = _git(repo_path, *args)
    return result.stdout


def build_diff(repo_path: str, commit: str) -> Dict:
    repo_path = str(Path(repo_path).expanduser().resolve())
    parent = _get_parent(repo_path, commit)
    files_changed, lines_added, lines_deleted = _collect_numstat(repo_path, parent, commit)
    patch = _collect_patch(repo_path, parent, commit)
    return {
        "commit": commit,
        "parent": parent,
        "files_changed": files_changed,
        "lines_added": lines_added,
        "lines_deleted": lines_deleted,
        "patch": patch,
    }


def write_diff(repo_path: str, commit: str, out_path: str):
    data = build_diff(repo_path, commit)
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Wrote diff metadata to {out_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate diff metadata for a commit.")
    parser.add_argument("--repo_path", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    write_diff(args.repo_path, args.commit, args.out)

