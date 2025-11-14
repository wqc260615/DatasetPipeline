"""Commit slicing strategies."""

from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional


def _parse_interval(interval: Optional[str]) -> Optional[timedelta]:
    if not interval:
        return None
    interval = interval.strip().lower()
    if interval.endswith("d"):
        return timedelta(days=int(interval[:-1]))
    if interval.endswith("w"):
        return timedelta(weeks=int(interval[:-1]))
    if interval.endswith("h"):
        return timedelta(hours=int(interval[:-1]))
    if interval.endswith("m"):
        return timedelta(minutes=int(interval[:-1]))
    raise ValueError(f"Unsupported interval format: {interval}")


def _is_release_tag(tag: str) -> bool:
    tag = tag.lower()
    return tag.startswith("v") or any(char.isdigit() for char in tag)


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _apply_limit(items: List[Dict], limit: Optional[int]) -> List[Dict]:
    if limit is None:
        return items
    return items[: max(limit, 0)]


def _slice_commit_mode(commits: List[Dict]) -> List[Dict]:
    return commits


def _slice_tag_mode(commits: List[Dict]) -> List[Dict]:
    return [c for c in commits if c.get("tags")]


def _slice_release_mode(commits: List[Dict]) -> List[Dict]:
    def has_release_tag(tags: Iterable[str]) -> bool:
        return any(_is_release_tag(tag) for tag in tags)

    return [c for c in commits if has_release_tag(c.get("tags", []))]


def _slice_interval_mode(commits: List[Dict], interval: timedelta) -> List[Dict]:
    if not interval:
        return commits
    if not commits:
        return []
    ordered = sorted(commits, key=lambda c: c["timestamp"])
    selected = []
    last_ts: Optional[datetime] = None
    for commit in ordered:
        ts = _parse_timestamp(commit["timestamp"])
        if last_ts is None or ts - last_ts >= interval:
            selected.append(commit)
            last_ts = ts
    selected.sort(key=lambda c: c["timestamp"], reverse=True)
    return selected


def slice_commits(
    commits: List[Dict],
    mode: str = "commit",
    interval: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict]:
    """Return a filtered subset of commits according to the requested mode."""
    mode = mode.lower()
    commits_sorted = sorted(commits, key=lambda c: c["timestamp"], reverse=True)
    if mode == "commit":
        sliced = _slice_commit_mode(commits_sorted)
    elif mode == "tag":
        sliced = _slice_tag_mode(commits_sorted)
    elif mode == "release":
        sliced = _slice_release_mode(commits_sorted)
    elif mode == "time-interval":
        delta = _parse_interval(interval)
        if delta is None:
            raise ValueError("time-interval mode requires a valid interval string (e.g. '30d').")
        sliced = _slice_interval_mode(commits_sorted, delta)
    else:
        raise ValueError(f"Unsupported slice mode: {mode}")

    return _apply_limit(sliced, limit)


__all__ = ["slice_commits"]

