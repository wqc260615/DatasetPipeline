import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pydriller import Repository


def _collect_tags(repo_path: str) -> Dict[str, List[str]]:
    """Return a mapping from commit hash to the tag names that reference it."""
    tags: Dict[str, List[str]] = {}
    try:
        result = subprocess.run(
            ["git", "-C", repo_path, "show-ref", "--tags", "-d"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return tags

    pending: Dict[str, str] = {}
    for line in result.stdout.strip().splitlines():
        if not line.strip():
            continue
        sha, ref = line.split()
        if ref.endswith("^{}"):
            tag_name = ref.split("refs/tags/")[-1][:-3]
            tags.setdefault(sha, []).append(tag_name)
            pending.pop(tag_name, None)
        else:
            tag_name = ref.split("refs/tags/")[-1]
            pending[tag_name] = sha

    for tag_name, sha in pending.items():
        tags.setdefault(sha, []).append(tag_name)

    return tags


def index_repo(repo_path: str, limit_commits: Optional[int] = None, sort_order: str = "desc"):
    """Scan the repository history and return commit metadata sorted by timestamp."""
    repo_path = str(Path(repo_path).expanduser().resolve())
    commits: List[Dict] = []
    tag_lookup = _collect_tags(repo_path)
    traversal = Repository(path_to_repo=repo_path)

    for commit in traversal.traverse_commits():
        parent_hash = None
        if commit.parents:
            first_parent = commit.parents[0]
            if hasattr(first_parent, "hash"):
                parent_hash = first_parent.hash
            elif isinstance(first_parent, str):
                parent_hash = first_parent
        commit_info = {
            "hash": commit.hash,
            "parent": parent_hash,
            "author": commit.author.name,
            "author_email": commit.author.email,
            "timestamp": commit.author_date.astimezone(timezone.utc).isoformat(),
            "message": commit.msg.splitlines()[0] if commit.msg else "",
            "tags": tag_lookup.get(commit.hash, []),
        }
        commits.append(commit_info)
        if limit_commits and len(commits) >= limit_commits:
            break

    reverse = sort_order != "asc"
    commits.sort(key=lambda c: c["timestamp"], reverse=reverse)
    return commits


def _ensure_parent_dir(path: str):
    out_path = Path(path)
    if out_path.suffix:
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        out_path.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Index repository history into JSON metadata.")
    parser.add_argument("--repo_path", required=True, help="Path to the git repository to scan")
    parser.add_argument("--out", default="./data/index.json", help="Path to write index JSON")
    parser.add_argument(
        "--limit_commits",
        type=int,
        default=1000,
        help="Optional cap on the number of commits to record",
    )
    parser.add_argument(
        "--sort_order",
        choices=["asc", "desc"],
        default="desc",
        help="Chronological sorting order for the commit list",
    )
    args = parser.parse_args()

    _ensure_parent_dir(args.out)
    commits = index_repo(args.repo_path, limit_commits=args.limit_commits, sort_order=args.sort_order)
    payload = {
        "repo_path": str(Path(args.repo_path).resolve()),
        "commit_count": len(commits),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "commits": commits,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"Wrote {args.out}")
