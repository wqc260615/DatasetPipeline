"""
Module: repository_cloner.py

Purpose: Clones Git repositories with full clone strategy and error handling.

Key Functions:
- clone_repository(url: str, target_dir: str) -> str
- validate_repository(repo_path: str) -> bool

Example:
    >>> repo_path = clone_repository("https://github.com/user/repo", "./data/repos/repo")
    >>> print(repo_path)
    ./data/repos/repo
"""

import logging
import shutil
import sys
from pathlib import Path
from typing import Optional
from git import Repo, GitCommandError
from git.exc import InvalidGitRepositoryError

logger = logging.getLogger(__name__)


def clone_repository(
    url: str,
    target_dir: str,
    max_retries: int = 3,
    existing_repo_action: str = "ask"
) -> Optional[str]:
    """
    Clone a Git repository with retry logic.
    
    Args:
        url: Repository URL (HTTPS or SSH)
        target_dir: Target directory for cloning
        max_retries: Maximum number of retry attempts
        existing_repo_action: Action when target exists.
            - "update": run git pull and reuse repository
            - "skip": reuse existing repository without updating
            - "ask": prompt user to choose update or skip
        
    Returns:
        Path to cloned repository, or None if cloning failed
        
    Raises:
        GitCommandError: If cloning fails after all retries
    """
    target_path = Path(target_dir)
    
    # Handle existing directory
    if target_path.exists():
        try:
            Repo(str(target_path))
            repo_exists = True
        except InvalidGitRepositoryError:
            repo_exists = False

        if repo_exists:
            action = existing_repo_action.lower().strip()
            if action not in {"ask", "update", "skip"}:
                logger.warning(
                    f"Unknown existing_repo_action '{existing_repo_action}', fallback to 'ask'"
                )
                action = "ask"

            if action == "ask":
                if sys.stdin.isatty():
                    answer = input(
                        f"Repository already exists at {target_path}. Update from remote? [y/N]: "
                    ).strip().lower()
                    action = "update" if answer in {"y", "yes"} else "skip"
                else:
                    logger.info(
                        "Repository exists and terminal is non-interactive; defaulting to skip update"
                    )
                    action = "skip"

            if action == "skip":
                logger.info(f"Using existing repository without update: {target_path}")
                return str(target_path)

            logger.info(f"Updating existing repository: {target_path}")
            for attempt in range(1, max_retries + 1):
                try:
                    repo = Repo(str(target_path))
                    if repo.remotes:
                        repo.remotes.origin.pull()
                    logger.info(f"Successfully updated repository: {target_path}")
                    return str(target_path)
                except Exception as e:
                    logger.error(f"Failed to update repository on attempt {attempt}: {e}")
                    if attempt == max_retries:
                        raise
        else:
            logger.warning(f"Target directory exists but is not a Git repo, removing: {target_path}")
            shutil.rmtree(target_path)
    
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Cloning repository (attempt {attempt}/{max_retries}): {url}")
            Repo.clone_from(url, str(target_path))
            
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
