"""
Module: semantic_slicer.py

Purpose: Identifies semantic evolution slices from Git commit history.

Key Functions:
- identify_slices(repo_path: str, config: Config) -> List[SemanticSlice]
- score_commit(commit: CommitInfo, repo: Repo, config: SlicingConfig) -> tuple[float, Optional[SliceType]]
- merge_close_slices(slices: List[SemanticSlice], min_interval: timedelta) -> List[SemanticSlice]

Example:
    >>> from pipeline.config import load_config
    >>> config = load_config()
    >>> slices = identify_slices("/path/to/repo", config)
    >>> print(len(slices))
    12
"""

import logging
import re
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from pathlib import Path
from git import Repo

from pipeline.models import CommitInfo, SemanticSlice, SliceType, SliceMetadata
from pipeline.config import Config, SlicingConfig
from pipeline.commit_extractor import (
    parse_version_tag,
    get_changed_files,
    detect_file_renames
)

logger = logging.getLogger(__name__)

# Patterns to identify non-code files
DOCUMENTATION_PATTERNS = [
    r'^docs?/',  # docs/, doc/
    r'^documentation/',
    r'\.md$',  # Markdown files
    r'\.rst$',  # reStructuredText
    r'\.txt$',  # Text files (often docs)
    r'^README',
    r'^CHANGELOG',
    r'^LICENSE',
    r'^CONTRIBUTING',
]

TRANSLATION_PATTERNS = [
    r'^i18n/',
    r'^locale/',
    r'^translations/',
    r'^lang/',
    r'^l10n/',
    r'/i18n/',
    r'/locale/',
    r'/translations/',
    r'/lang/',
    r'/l10n/',
    r'\.po$',  # Gettext translation files
    r'\.pot$',  # Gettext template files
]

# Commit message patterns that indicate documentation/translation updates
DOC_COMMIT_PATTERNS = [
    r'\[docs?\]',
    r'\[documentation\]',
    r'docs?:',
    r'documentation:',
    r'update.*readme',
    r'update.*changelog',
    r'fix.*typo',
    r'fix.*grammar',
    r'fix.*spelling',
]

TRANSLATION_COMMIT_PATTERNS = [
    r'\[i18n',
    r'\[translation',
    r'\[locale',
    r'translate',
    r'translation',
    r'i18n',
    r'locale',
]


def is_documentation_or_translation_commit(commit: CommitInfo, repo: Repo) -> bool:
    """
    Check if a commit is primarily documentation or translation updates.
    
    Args:
        commit: Commit information
        repo: Git repository object
        
    Returns:
        True if commit is primarily docs/translation, False otherwise
    """
    # Check commit message for documentation/translation indicators
    message_lower = commit.message.lower()
    
    # Check for documentation commit patterns
    if any(re.search(pattern, message_lower) for pattern in DOC_COMMIT_PATTERNS):
        return True
    
    # Check for translation commit patterns
    if any(re.search(pattern, message_lower) for pattern in TRANSLATION_COMMIT_PATTERNS):
        return True
    
    # Check changed files
    try:
        changed_files = get_changed_files(repo, commit.hash)
        if not changed_files:
            return False
        
        # Count code vs non-code files
        code_files = 0
        doc_files = 0
        translation_files = 0
        
        for file_path in changed_files:
            file_path_lower = file_path.lower()
            
            # Check if it's a documentation file
            if any(re.search(pattern, file_path_lower) for pattern in DOCUMENTATION_PATTERNS):
                doc_files += 1
                continue
            
            # Check if it's a translation file
            if any(re.search(pattern, file_path_lower) for pattern in TRANSLATION_PATTERNS):
                translation_files += 1
                continue
            
            # Check if it's a code file (common extensions)
            code_extensions = ['.py', '.java', '.js', '.ts', '.cpp', '.c', '.h', '.go', '.rs', '.rb', '.php']
            if any(file_path_lower.endswith(ext) for ext in code_extensions):
                code_files += 1
        
        total_files = len(changed_files)
        
        # If more than 80% of files are docs/translation, consider it non-code
        non_code_ratio = (doc_files + translation_files) / total_files if total_files > 0 else 0
        
        # Also check if there are significant code changes
        # If less than 20% are code files, it's likely a docs/translation commit
        code_ratio = code_files / total_files if total_files > 0 else 0
        
        # Exclude if:
        # 1. More than 80% are docs/translation files, OR
        # 2. Less than 20% are code files AND there are docs/translation files
        if non_code_ratio > 0.8 or (code_ratio < 0.2 and (doc_files + translation_files) > 0):
            return True
        
        # Special case: if commit message has translation indicators and files are mostly translation
        if translation_files > 0 and code_ratio < 0.3:
            return True
            
    except Exception as e:
        logger.warning(f"Error checking files for commit {commit.hash[:8]}: {e}")
        # If we can't check files, rely on commit message only
        return False
    
    return False


def identify_slices(repo_path: str, config: Config) -> List[SemanticSlice]:
    """
    Identify semantic evolution slices from a repository.
    
    Args:
        repo_path: Path to Git repository
        config: Configuration object
        
    Returns:
        List of SemanticSlice objects
    """
    logger.info(f"Identifying slices for repository: {repo_path}")
    
    try:
        repo = Repo(repo_path)
        commits = _extract_commits_for_slicer(repo_path)
        
        if not commits:
            logger.warning(f"No commits found in repository: {repo_path}")
            return []
        
        # Score commits for slice candidacy
        scored_commits = []
        for commit in commits:
            score, slice_type = score_commit(commit, repo, config.slicing)
            if score >= config.slicing.slice_score_threshold:
                scored_commits.append((commit, score, slice_type))
        
        logger.info(f"Found {len(scored_commits)} candidate slices")
        
        # Convert to SemanticSlice objects
        slices = []
        for commit, score, slice_type in scored_commits:
            try:
                slice_obj = create_slice_from_commit(commit, repo, slice_type, config)
                if slice_obj:
                    slices.append(slice_obj)
            except Exception as e:
                logger.warning(f"Error creating slice for commit {commit.hash[:8]}: {e}")
                continue
        
        # Apply temporal filtering
        slices = merge_close_slices(slices, timedelta(days=config.slicing.min_interval_days))
        
        # Limit number of slices
        if len(slices) > config.slicing.max_slices_per_repo:
            # Keep highest scoring slices
            slices.sort(key=lambda s: get_slice_score(s, scored_commits), reverse=True)
            slices = slices[:config.slicing.max_slices_per_repo]
            slices.sort(key=lambda s: s.commit_date)  # Re-sort by date
        
        logger.info(f"Final slice count: {len(slices)}")
        return slices
        
    except Exception as e:
        logger.error(f"Error identifying slices for {repo_path}: {e}")
        return []


def score_commit(
    commit: CommitInfo,
    repo: Repo,
    config: SlicingConfig
) -> Tuple[float, Optional[SliceType]]:
    """
    Score a commit for slice candidacy using weighted criteria.
    
    Args:
        commit: Commit information
        repo: Git repository object
        config: Slicing configuration
        
    Returns:
        Tuple of (score, slice_type) where score is 0.0-1.0
    """
    # Exclude documentation and translation commits (unless they're version releases)
    # Version releases should still be included even if they contain docs
    is_docs_or_translation = is_documentation_or_translation_commit(commit, repo)
    has_version_tag = bool(commit.tags and any(
        parse_version_tag(tag) for tag in commit.tags
    ))
    
    # If it's a docs/translation commit and NOT a version release, exclude it
    if is_docs_or_translation and not has_version_tag:
        logger.debug(f"Excluding docs/translation commit {commit.hash[:8]}: {commit.message[:50]}...")
        return 0.0, None
    
    score = 0.0
    slice_type = None
    
    # Priority 1: Version releases (highest weight)
    if commit.tags:
        for tag in commit.tags:
            version_info = parse_version_tag(tag)
            if version_info:
                version_type = version_info["type"]
                weight = config.version_release_weights.get(version_type, 0.3)
                if weight > score:
                    score = weight
                    slice_type = SliceType.VERSION_RELEASE
    
    # Priority 2: Major feature integrations
    if is_major_feature(commit, config):
        feature_score = 0.6
        if feature_score > score:
            score = feature_score
            slice_type = slice_type or SliceType.FEATURE
    
    # Priority 3: Breaking API changes
    if has_api_changes(commit, repo):
        api_score = 0.5
        if api_score > score:
            score = api_score
            slice_type = slice_type or SliceType.API_CHANGE
    
    # Priority 4: Large-scale refactoring
    if is_large_refactoring(commit, repo, config):
        refactor_score = 0.4
        if refactor_score > score:
            score = refactor_score
            slice_type = slice_type or SliceType.REFACTORING
    
    return score, slice_type


def is_major_feature(commit: CommitInfo, config: SlicingConfig) -> bool:
    """
    Check if commit represents a major feature integration.
    
    Args:
        commit: Commit information
        config: Slicing configuration
        
    Returns:
        True if major feature, False otherwise
    """
    # Check commit message for feature markers
    message_lower = commit.message.lower()
    feature_markers = ["feat:", "feature", "add", "implement", "introduce"]
    
    has_feature_marker = any(marker in message_lower for marker in feature_markers)
    
    # Check size thresholds
    meets_size_threshold = (
        commit.lines_added > config.major_feature_threshold_lines or
        commit.files_changed > 10
    )
    
    return has_feature_marker and meets_size_threshold


def has_api_changes(commit: CommitInfo, repo: Repo) -> bool:
    """
    Detect breaking API changes in a commit.
    
    Args:
        commit: Commit information
        repo: Git repository object
        
    Returns:
        True if API changes detected, False otherwise
    """
    try:
        changed_files = get_changed_files(repo, commit.hash)
        
        # Look for public interface files
        api_indicators = [
            "__init__.py",  # Python packages
            "index.ts", "index.js",  # JS/TS entry points
            "*.java"  # Java public classes
        ]
        
        # Check if commit modifies public interfaces
        # This is a simplified check - full implementation would parse ASTs
        for file_path in changed_files:
            if any(indicator in file_path for indicator in api_indicators):
                # Check commit message for API-related keywords
                message_lower = commit.message.lower()
                api_keywords = ["api", "interface", "signature", "breaking", "deprecate"]
                if any(keyword in message_lower for keyword in api_keywords):
                    return True
        
        return False
        
    except Exception as e:
        logger.warning(f"Error checking API changes for commit {commit.hash[:8]}: {e}")
        return False


def is_large_refactoring(commit: CommitInfo, repo: Repo, config: SlicingConfig) -> bool:
    """
    Detect large-scale refactoring events.
    
    Args:
        commit: Commit information
        repo: Git repository object
        config: Slicing configuration
        
    Returns:
        True if large refactoring detected, False otherwise
    """
    try:
        # Check for file renames
        renames = detect_file_renames(repo, commit.hash)
        if len(renames) >= config.refactoring_file_threshold:
            return True
        
        # Check commit message for refactoring keywords
        message_lower = commit.message.lower()
        refactor_keywords = ["refactor", "restructure", "reorganize", "move", "rename"]
        if any(keyword in message_lower for keyword in refactor_keywords):
            # Check if many files changed
            if commit.files_changed >= config.refactoring_file_threshold:
                return True
        
        return False
        
    except Exception as e:
        logger.warning(f"Error checking refactoring for commit {commit.hash[:8]}: {e}")
        return False


def create_slice_from_commit(
    commit: CommitInfo,
    repo: Repo,
    slice_type: SliceType,
    config: Config
) -> Optional[SemanticSlice]:
    """
    Create a SemanticSlice object from a commit.
    
    Args:
        commit: Commit information
        repo: Git repository object
        slice_type: Type of semantic slice
        config: Configuration object
        
    Returns:
        SemanticSlice object, or None if error
    """
    try:
        # Generate slice ID
        repo_name = Path(repo.working_dir).name
        date_str = commit.date.strftime("%Y%m%d")
        slice_id = f"{repo_name}_{commit.hash[:8]}_{date_str}"
        
        # Get version tag if available
        version_tag = None
        if commit.tags:
            for tag in commit.tags:
                if parse_version_tag(tag):
                    version_tag = tag
                    break
        
        # Get changed files
        changed_files = get_changed_files(repo, commit.hash)
        
        # Create metadata
        metadata = SliceMetadata(
            total_files=commit.files_changed,
            total_lines=commit.lines_added + commit.lines_deleted,
            changed_files_since_prev_slice=commit.files_changed,
            commit_message=commit.message,
            lines_added=commit.lines_added,
            lines_deleted=commit.lines_deleted,
            files_modified=changed_files
        )
        
        slice_obj = SemanticSlice(
            slice_id=slice_id,
            commit_hash=commit.hash,
            commit_date=commit.date.isoformat(),
            slice_type=slice_type,
            version_tag=version_tag,
            files=[],  # Files will be populated by AST parser
            metadata=metadata
        )
        
        return slice_obj
        
    except Exception as e:
        logger.error(f"Error creating slice from commit {commit.hash[:8]}: {e}")
        return None


def merge_close_slices(
    slices: List[SemanticSlice],
    min_interval: timedelta
) -> List[SemanticSlice]:
    """
    Merge slices that are too close together temporally.
    
    Args:
        slices: List of slices
        min_interval: Minimum time interval between slices
        
    Returns:
        Filtered list of slices
    """
    if not slices:
        return []
    
    # Sort by date
    slices_sorted = sorted(slices, key=lambda s: s.commit_date)
    
    merged = [slices_sorted[0]]
    
    for current_slice in slices_sorted[1:]:
        last_slice = merged[-1]
        
        # Parse dates
        last_date = datetime.fromisoformat(last_slice.commit_date.replace('Z', '+00:00'))
        current_date = datetime.fromisoformat(current_slice.commit_date.replace('Z', '+00:00'))
        
        time_diff = current_date - last_date
        
        if time_diff >= min_interval:
            merged.append(current_slice)
        else:
            # Merge: keep the slice with higher priority (version_release > feature > api_change > refactoring)
            priority_order = {
                SliceType.VERSION_RELEASE: 4,
                SliceType.FEATURE: 3,
                SliceType.API_CHANGE: 2,
                SliceType.REFACTORING: 1
            }
            
            if priority_order.get(current_slice.slice_type, 0) > priority_order.get(last_slice.slice_type, 0):
                merged[-1] = current_slice
    
    return merged


def get_slice_score(slice: SemanticSlice, scored_commits: List[Tuple]) -> float:
    """
    Get the score for a slice (for sorting purposes).
    
    Args:
        slice: Semantic slice
        scored_commits: List of (commit, score, slice_type) tuples
        
    Returns:
        Score value
    """
    for commit, score, _ in scored_commits:
        if commit.hash == slice.commit_hash:
            return score
    return 0.0


def _extract_commits_for_slicer(repo_path: str) -> List[CommitInfo]:
    """
    Helper function to extract commits (imports from commit_extractor).
    
    Args:
        repo_path: Path to repository
        
    Returns:
        List of CommitInfo objects
    """
    from pipeline.commit_extractor import extract_commits as _extract_commits
    return _extract_commits(repo_path)
