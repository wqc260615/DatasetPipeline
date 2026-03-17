"""
Module: commit_extractor.py

Purpose: Parses Git tags and computes diffs for semantic slicing.

Key Functions:
- parse_release_tag(tag_name: str) -> Tuple[Optional[Tuple[int, int, int]], str]
- get_diff_between_refs(repo: Repo, old_ref: str, new_ref: str)
"""

import logging
import re
from typing import Optional, Tuple
from git import Repo
from git.exc import GitCommandError

logger = logging.getLogger(__name__)


def get_diff_between_refs(repo: Repo, old_ref: str, new_ref: str):
    try:
        old_commit = repo.commit(old_ref)
        new_commit = repo.commit(new_ref)
        return old_commit.diff(new_commit, create_patch=True)
    except Exception as e:
        logger.warning(f"Error getting diff between {old_ref[:8]} and {new_ref[:8]}: {e}")
        return None


def parse_release_tag(tag_name: str) -> Tuple[Optional[Tuple[int, int, int]], str]:
    """
    Parse release-like tags with lenient version extraction and strict remainder check.

    Accepts tags like ``v1.2.3``, ``1.2.3``, ``v1.1``, ``1.1``, ``v2``,
    ``release-1.2``, ``1.2-release``.  Rejects any tag whose version number
    is followed by a non-empty remainder (rc, alpha, preview, model names, etc.).

    All version comparisons and sorting MUST use the returned integer tuple –
    never compare version strings lexicographically.

    Args:
        tag_name: Raw Git tag string.

    Returns:
        ``(version_tuple, "release")`` on success, or ``(None, "discard")``.
    """
    cleaned = re.sub(r'^[vV]', '', tag_name)
    # Strip 'release' prefix: release-1.2, release_1.2, release.1.2
    cleaned = re.sub(r'^release[-_.]?', '', cleaned, flags=re.IGNORECASE)
    # Strip 'release' suffix: 1.2-release, 1.2_release
    cleaned = re.sub(r'[-_.]?release$', '', cleaned, flags=re.IGNORECASE)

    m = re.match(r'^(\d+)(?:\.(\d+))?(?:\.(\d+))?', cleaned)
    if not m:
        return None, "discard"

    version = (int(m.group(1)), int(m.group(2) or 0), int(m.group(3) or 0))
    remainder = cleaned[m.end():]

    if remainder:
        return None, "discard"

    return version, "release"
