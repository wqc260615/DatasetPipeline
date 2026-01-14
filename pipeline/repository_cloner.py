"""
Module: repository_cloner.py

Purpose: Clones Git repositories with shallow cloning support and error handling.

Key Functions:
- clone_repository(url: str, target_dir: str, shallow: bool = True) -> str
- validate_repository(repo_path: str) -> bool

Example:
    >>> repo_path = clone_repository("https://github.com/user/repo", "./data/repos/repo")
    >>> print(repo_path)
    ./data/repos/repo
"""

import logging
import shutil
from pathlib import Path
from typing import Optional
from git import Repo, GitCommandError
from git.exc import InvalidGitRepositoryError

logger = logging.getLogger(__name__)


def clone_repository(
    url: str,
    target_dir: str,
    shallow: bool = True,
    depth: int = 1000,
    max_retries: int = 3
) -> Optional[str]:
    """
    Clone a Git repository with retry logic and shallow cloning support.
    
    Args:
        url: Repository URL (HTTPS or SSH)
        target_dir: Target directory for cloning
        shallow: Whether to use shallow cloning
        depth: Number of commits to fetch (for shallow clones)
        max_retries: Maximum number of retry attempts
        
    Returns:
        Path to cloned repository, or None if cloning failed
        
    Raises:
        GitCommandError: If cloning fails after all retries
    """
    target_path = Path(target_dir)
    
    # Remove existing directory if it exists
    if target_path.exists():
        logger.warning(f"Target directory exists, removing: {target_path}")
        shutil.rmtree(target_path)
    
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Cloning repository (attempt {attempt}/{max_retries}): {url}")
            
            if shallow:
                repo = Repo.clone_from(
                    url,
                    str(target_path),
                    depth=depth,
                    single_branch=True
                )
                # Fetch all tags even if their commits are outside shallow clone depth
                # This ensures version releases can be identified even for older commits
                logger.info("Fetching all tags from remote repository...")
                try:
                    # Fetch all tags (this gets tag objects and their metadata)
                    # Note: This may not fetch the commits themselves if they're outside shallow clone depth
                    repo.git.fetch('--tags', '--force')
                    tag_count = len(list(repo.tags))
                    logger.info(f"Fetched {tag_count} tags from remote repository")
                    
                    # Try to fetch commits that tags point to (for version release identification)
                    # We'll try to fetch each tag's commit individually with depth=1
                    logger.info("Attempting to fetch commits referenced by version tags...")
                    fetched_commits = 0
                    skipped_commits = 0
                    max_tag_commits_to_fetch = 1000  # Limit to avoid excessive fetching
                    
                    for tag_ref in repo.tags:
                        if fetched_commits >= max_tag_commits_to_fetch:
                            logger.info(f"Reached limit of {max_tag_commits_to_fetch} tag commits to fetch")
                            break
                        try:
                            # Get the commit hash this tag points to
                            tag_commit_hash = tag_ref.commit.hexsha
                            
                            # Check if commit is already in local repo
                            try:
                                repo.commit(tag_commit_hash)
                                # Commit already exists, skip
                                continue
                            except (ValueError, GitCommandError):
                                # Commit not in local repo, try to fetch it
                                try:
                                    # Try to fetch the commit by its hash with depth=1
                                    # This may not work for very old commits, but worth trying
                                    tag_name = tag_ref.name
                                    # Fetch the tag again with depth=1 to try to get its commit
                                    repo.git.fetch('origin', f'refs/tags/{tag_name}:refs/tags/{tag_name}', '--depth=1')
                                    # Verify the commit is now accessible
                                    try:
                                        repo.commit(tag_commit_hash)
                                        fetched_commits += 1
                                    except (ValueError, GitCommandError):
                                        skipped_commits += 1
                                except GitCommandError:
                                    # Commit might be too old or not accessible, skip
                                    skipped_commits += 1
                                    continue
                        except Exception:
                            # Tag might be lightweight or invalid, skip
                            skipped_commits += 1
                            continue
                    
                    if fetched_commits > 0:
                        logger.info(f"Successfully fetched {fetched_commits} additional commits referenced by tags")
                    if skipped_commits > 0:
                        logger.info(f"Skipped {skipped_commits} tags whose commits are not accessible (likely outside shallow clone range)")
                except GitCommandError as e:
                    logger.warning(f"Failed to fetch tags (non-critical): {e}")
            else:
                repo = Repo.clone_from(url, str(target_path))
            
            logger.info(f"Successfully cloned repository to: {target_path}")
            return str(target_path)
            
        except GitCommandError as e:
            logger.error(f"Git command error on attempt {attempt}: {e}")
            if attempt == max_retries:
                logger.error(f"Failed to clone repository after {max_retries} attempts")
                raise
            # Clean up partial clone
            if target_path.exists():
                shutil.rmtree(target_path)
                
        except Exception as e:
            logger.error(f"Unexpected error during clone (attempt {attempt}): {e}")
            if attempt == max_retries:
                logger.error(f"Failed to clone repository after {max_retries} attempts")
                raise
            if target_path.exists():
                shutil.rmtree(target_path)
    
    return None


def validate_repository(repo_path: str) -> bool:
    """
    Validate that a path contains a valid Git repository.
    
    Args:
        repo_path: Path to repository
        
    Returns:
        True if valid repository, False otherwise
    """
    try:
        repo = Repo(repo_path)
        # Check if repository has commits
        if len(list(repo.iter_commits())) == 0:
            logger.warning(f"Repository has no commits: {repo_path}")
            return False
        return True
    except InvalidGitRepositoryError:
        logger.error(f"Invalid Git repository: {repo_path}")
        return False
    except Exception as e:
        logger.error(f"Error validating repository {repo_path}: {e}")
        return False


def get_repository_info(repo_path: str) -> Optional[dict]:
    """
    Extract basic information about a repository.
    
    Args:
        repo_path: Path to repository
        
    Returns:
        Dictionary with repository info, or None if error
    """
    try:
        repo = Repo(repo_path)
        remotes = repo.remotes
        
        url = None
        if remotes:
            url = remotes[0].url
        
        return {
            "path": repo_path,
            "url": url,
            "branch": repo.active_branch.name if repo.head.is_valid() else None,
            "commit_count": len(list(repo.iter_commits())),
            "is_bare": repo.bare
        }
    except Exception as e:
        logger.error(f"Error getting repository info: {e}")
        return None
