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
from typing import List, Optional
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
        
        # Also process commits referenced by tags that might not be in shallow clone
        # This ensures version releases are identified even if their commits are old
        logger.info("Checking tags for commits outside shallow clone range...")
        tag_commits_added = 0
        for tag_ref in repo.tags:
            try:
                tag_commit_hash = tag_ref.commit.hexsha
                
                # Skip if we already processed this commit
                if tag_commit_hash in processed_hashes:
                    continue
                
                # Try to access the commit (it might have been fetched by repository_cloner)
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
                    # Commit not accessible (too old or not fetched), skip
                    # This is expected for tags pointing to commits outside shallow clone
                    continue
                    
            except Exception as e:
                # Tag might be invalid, skip
                continue
        
        if tag_commits_added > 0:
            logger.info(f"Added {tag_commits_added} commits referenced by tags that were outside shallow clone range")
        
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


def parse_version_tag(tag: str) -> Optional[dict]:
    """
    Parse a version tag to extract version information.
    
    Supports semver patterns: v1.2.3, 1.2.3, v1.2.3-beta, etc.
    
    Args:
        tag: Git tag string
        
    Returns:
        Dictionary with version info, or None if not a version tag
    """
    # Remove 'v' prefix if present
    tag_clean = tag.lstrip('v')
    
    # Match semver pattern: major.minor.patch[-prerelease][+build]
    pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([\w\.-]+))?(?:\+([\w\.-]+))?$'
    match = re.match(pattern, tag_clean)
    
    if match:
        major, minor, patch, prerelease, build = match.groups()
        return {
            "major": int(major),
            "minor": int(minor),
            "patch": int(patch),
            "prerelease": prerelease,
            "build": build,
            "type": "major" if int(minor) == 0 and int(patch) == 0 else
                   "minor" if int(patch) == 0 else "patch"
        }
    
    return None


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
