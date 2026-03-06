"""
Module: commit_extractor.py

Purpose: Extracts commit metadata, diffs, and messages from Git repositories.

Key Functions:
- extract_commits(repo_path: str) -> List[CommitInfo]
- get_commit_diff(repo: Repo, commit_hash: str) -> str
- get_tags_at_commit(repo: Repo, commit_hash: str) -> List[str]

Example:
    >>> commits = extract_commits("/path/to/repo")
    >>> print(len(commits))
    150
"""

import logging
import re
from datetime import datetime
from typing import List, Optional, Tuple
from git import Repo
from git.exc import GitCommandError

from pipeline.models import CommitInfo

logger = logging.getLogger(__name__)


def extract_commits(
    repo_path: str,
    max_commits: Optional[int] = None,
    since: Optional[datetime] = None
) -> List[CommitInfo]:
    """
    Extract commit information from a Git repository.
    
    Args:
        repo_path: Path to Git repository
        max_commits: Maximum number of commits to extract (None for all)
        since: Only extract commits after this date
        
    Returns:
        List of CommitInfo objects
    """
    try:
        repo = Repo(repo_path)
        commits = []
        
        # Get all commits
        commit_iter = repo.iter_commits()
        if max_commits:
            commit_iter = list(commit_iter)[:max_commits]
        
        # Track commit hashes we've already processed
        processed_hashes = set()
        
        for commit in commit_iter:
            # Filter by date if specified
            if since and commit.committed_datetime < since:
                continue
            
            try:
                # Get commit stats
                stats = commit.stats.total
                
                # Check if merge commit
                is_merge = len(commit.parents) > 1
                
                # Get tags for this commit
                tags = get_tags_at_commit(repo, commit.hexsha)
                
                commit_info = CommitInfo(
                    hash=commit.hexsha,
                    message=commit.message.strip(),
                    author=f"{commit.author.name} <{commit.author.email}>",
                    date=commit.committed_datetime,
                    files_changed=stats.get('files', 0),
                    lines_added=stats.get('insertions', 0),
                    lines_deleted=stats.get('deletions', 0),
                    is_merge=is_merge,
                    tags=tags
                )
                
                commits.append(commit_info)
                processed_hashes.add(commit.hexsha)
                
            except Exception as e:
                logger.warning(f"Error processing commit {commit.hexsha[:8]}: {e}")
                continue
        
        # Process commits referenced by tags from all branches
        # This ensures version releases from all branches are identified
        logger.info("Checking tags for commits from other branches...")        
        tag_commits_added = 0
        for tag_ref in repo.tags:
            try:
                tag_commit_hash = tag_ref.commit.hexsha
                
                # Skip if we already processed this commit
                if tag_commit_hash in processed_hashes:
                    continue
                
                # Try to access the commit directly from local repository data
                try:
                    commit = repo.commit(tag_commit_hash)
                    
                    # Filter by date if specified
                    if since and commit.committed_datetime < since:
                        continue
                    
                    # Get commit stats
                    stats = commit.stats.total
                    
                    # Check if merge commit
                    is_merge = len(commit.parents) > 1
                    
                    # Get tags for this commit
                    tags = get_tags_at_commit(repo, commit.hexsha)
                    
                    commit_info = CommitInfo(
                        hash=commit.hexsha,
                        message=commit.message.strip(),
                        author=f"{commit.author.name} <{commit.author.email}>",
                        date=commit.committed_datetime,
                        files_changed=stats.get('files', 0),
                        lines_added=stats.get('insertions', 0),
                        lines_deleted=stats.get('deletions', 0),
                        is_merge=is_merge,
                        tags=tags
                    )
                    
                    commits.append(commit_info)
                    processed_hashes.add(commit.hexsha)
                    tag_commits_added += 1
                    
                except (ValueError, GitCommandError):
                    # Commit not accessible in local repository, skip
                    continue
                    
            except Exception as e:
                # Tag might be invalid, skip
                continue
        
        if tag_commits_added > 0:
            logger.info(f"Added {tag_commits_added} commits referenced by tags from other branches")
        
        logger.info(f"Extracted {len(commits)} commits from {repo_path}")
        return commits
        
    except Exception as e:
        logger.error(f"Error extracting commits from {repo_path}: {e}")
        return []


def get_commit_diff(repo: Repo, commit_hash: str) -> Optional[str]:
    """
    Get the diff for a specific commit.
    
    Args:
        repo: Git repository object
        commit_hash: Commit hash
        
    Returns:
        Diff string, or None if error
    """
    try:
        commit = repo.commit(commit_hash)
        if len(commit.parents) > 0:
            return commit.parents[0].diff(commit, create_patch=True)
        else:
            # Initial commit
            return commit.diff(create_patch=True)
    except Exception as e:
        logger.warning(f"Error getting diff for commit {commit_hash[:8]}: {e}")
        return None


def get_diff_between_refs(repo: Repo, old_ref: str, new_ref: str):
    try:
        old_commit = repo.commit(old_ref)
        new_commit = repo.commit(new_ref)
        return old_commit.diff(new_commit, create_patch=True)
    except Exception as e:
        logger.warning(f"Error getting diff between {old_ref[:8]} and {new_ref[:8]}: {e}")
        return None


def get_tags_at_commit(repo: Repo, commit_hash: str) -> List[str]:
    """
    Get all Git tags pointing to a specific commit.
    
    Args:
        repo: Git repository object
        commit_hash: Commit hash
        
    Returns:
        List of tag names
    """
    tags = []
    try:
        for tag_ref in repo.tags:
            try:
                if tag_ref.commit.hexsha == commit_hash:
                    tags.append(tag_ref.name)
            except Exception:
                # Some tags might be lightweight and not have commits
                continue
    except Exception as e:
        logger.warning(f"Error getting tags for commit {commit_hash[:8]}: {e}")
    
    return tags


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


def get_changed_files(repo: Repo, commit_hash: str) -> List[str]:
    """
    Get list of files changed in a commit.
    
    Args:
        repo: Git repository object
        commit_hash: Commit hash
        
    Returns:
        List of file paths
    """
    try:
        commit = repo.commit(commit_hash)
        changed_files = []
        
        if len(commit.parents) > 0:
            diff = commit.parents[0].diff(commit)
        else:
            diff = commit.diff(create_patch=True)
        
        for item in diff:
            if item.a_path:
                changed_files.append(item.a_path)
            if item.b_path and item.b_path != item.a_path:
                changed_files.append(item.b_path)
        
        return list(set(changed_files))  # Remove duplicates
        
    except Exception as e:
        logger.warning(f"Error getting changed files for commit {commit_hash[:8]}: {e}")
        return []


def detect_file_renames(repo: Repo, commit_hash: str) -> List[tuple]:
    """
    Detect file renames in a commit.
    
    Args:
        repo: Git repository object
        commit_hash: Commit hash
        
    Returns:
        List of (old_path, new_path) tuples
    """
    renames = []
    try:
        commit = repo.commit(commit_hash)
        if len(commit.parents) == 0:
            return renames
        
        diff = commit.parents[0].diff(commit, find_renames=True, find_copies=True)
        
        for item in diff:
            if item.renamed:
                renames.append((item.a_path, item.b_path))
    
    except Exception as e:
        logger.warning(f"Error detecting renames for commit {commit_hash[:8]}: {e}")
    
    return renames
