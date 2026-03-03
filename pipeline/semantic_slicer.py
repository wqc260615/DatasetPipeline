"""
Module: semantic_slicer.py

Purpose: Identifies semantic evolution slices from Git commit history.

Key Functions:
- identify_slices(repo_path: str, config: Config) -> List[SemanticSlice]
- score_commit(commit: CommitInfo, repo: Repo, config: SlicingConfig) -> tuple[float, Optional[SliceType], dict]
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
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from git import Repo

from pipeline.models import CommitInfo, SemanticSlice, SliceType, SliceMetadata
from pipeline.config import Config, SlicingConfig
from pipeline.commit_extractor import (
    parse_version_tag,
    get_changed_files,
    detect_file_renames,
    get_diff_between_refs
)

logger = logging.getLogger(__name__)

# Cache for parsed API symbols keyed by blob hash
_api_symbol_cache: Dict[str, Dict[str, Dict[Tuple, Tuple]]] = {}

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
            score, slice_type, breakdown = score_commit(commit, repo, config.slicing)
            if score >= config.slicing.slice_score_threshold:
                scored_commits.append((commit, score, slice_type, breakdown))
        
        logger.info(f"Found {len(scored_commits)} candidate slices")
        
        # Convert to SemanticSlice objects
        slices = []
        for commit, score, slice_type, breakdown in scored_commits:
            try:
                slice_obj = create_slice_from_commit(
                    commit,
                    repo,
                    slice_type,
                    config,
                    score,
                    breakdown
                )
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
) -> Tuple[float, Optional[SliceType], Dict[str, object]]:
    """
    Score a commit for slice candidacy using weighted criteria.
    
    Args:
        commit: Commit information
        repo: Git repository object
        config: Slicing configuration
        
    Returns:
        Tuple of (score, slice_type, breakdown)
    """
    # Exclude documentation and translation commits (unless they're version releases)
    # Version releases should still be included even if they contain docs
    is_docs_or_translation = is_documentation_or_translation_commit(commit, repo)
    has_version_tag = bool(commit.tags and any(
        parse_version_tag(tag) for tag in commit.tags
    ))
    
    breakdown: Dict[str, object] = {
        "excluded_docs_or_translation": False,
        "version_release": {},
        "major_feature": {},
        "api_change": {},
        "refactoring": {},
        "final": {}
    }

    # If it's a docs/translation commit and NOT a version release, exclude it
    if is_docs_or_translation and not has_version_tag:
        logger.debug(f"Excluding docs/translation commit {commit.hash[:8]}: {commit.message[:50]}...")
        breakdown["excluded_docs_or_translation"] = True
        breakdown["final"] = {
            "score": 0.0,
            "reason": "excluded_docs_or_translation",
            "slice_type": None
        }
        return 0.0, None, breakdown
    
    score = 0.0
    slice_type = None
    
    # Priority 1: Version releases (highest weight)
    version_release_matched = False
    version_release_weight = 0.0
    version_release_type = None
    version_release_tag = None
    if commit.tags:
        for tag in commit.tags:
            version_info = parse_version_tag(tag)
            if version_info:
                version_type = version_info["type"]
                weight = config.version_release_weights.get(version_type, 0.3)
                version_release_matched = True
                if weight >= version_release_weight:
                    version_release_weight = weight
                    version_release_type = version_type
                    version_release_tag = tag
                if weight > score:
                    score = weight
                    slice_type = SliceType.VERSION_RELEASE
    breakdown["version_release"] = {
        "matched": version_release_matched,
        "weight": version_release_weight,
        "version_type": version_release_type,
        "tag": version_release_tag
    }
    
    # Priority 2: Major feature integrations
    is_feature, feature_details = is_major_feature(commit, config)
    breakdown["major_feature"] = {
        **feature_details,
        "weight": 0.6
    }
    if is_feature:
        feature_score = 0.6
        if feature_score > score:
            score = feature_score
            slice_type = slice_type or SliceType.FEATURE
    
    # Priority 3: Breaking API changes
    has_api, api_details = has_api_changes(commit, repo)
    breakdown["api_change"] = {
        **api_details,
        "weight": 0.5
    }
    if has_api:
        api_score = 0.5
        if api_score > score:
            score = api_score
            slice_type = slice_type or SliceType.API_CHANGE
    
    # Priority 4: Large-scale refactoring
    is_refactor, refactor_details = is_large_refactoring(commit, repo, config)
    breakdown["refactoring"] = {
        **refactor_details,
        "weight": 0.4
    }
    if is_refactor:
        refactor_score = 0.4
        if refactor_score > score:
            score = refactor_score
            slice_type = slice_type or SliceType.REFACTORING

    breakdown["final"] = {
        "score": score,
        "reason": slice_type.value if slice_type else None,
        "slice_type": slice_type.value if slice_type else None
    }
    
    return score, slice_type, breakdown


def is_major_feature(commit: CommitInfo, config: SlicingConfig) -> Tuple[bool, Dict[str, object]]:
    """
    Check if commit represents a major feature integration.
    
    Args:
        commit: Commit information
        config: Slicing configuration
        
    Returns:
        Tuple of (is_major_feature, details)
    """
    # Check commit message for feature markers
    message_lower = commit.message.lower()
    feature_markers = ["feat:", "feature", "add", "implement", "introduce"]
    
    has_feature_marker = any(marker in message_lower for marker in feature_markers)
    
    # Check size thresholds
    total_lines_changed = commit.lines_added + commit.lines_deleted
    meets_size_threshold = (
        total_lines_changed > config.major_feature_threshold_lines or
        commit.files_changed > 10
    )
    
    details = {
        "matched": has_feature_marker and meets_size_threshold,
        "has_feature_marker": has_feature_marker,
        "meets_size_threshold": meets_size_threshold,
        "total_lines_changed": total_lines_changed,
        "files_changed": commit.files_changed,
        "threshold_lines": config.major_feature_threshold_lines
    }

    return details["matched"], details


def has_api_changes(commit: CommitInfo, repo: Repo) -> Tuple[bool, Dict[str, object]]:
    """
    Detect breaking API changes in a commit using lightweight AST symbol diffing.
    
    Strategy:
    - Only analyze changed code files (.py, .java)
    - Compare commit vs parent for public symbols (functions/classes)
    - Use lightweight symbol extraction (no full AST retention)
    - Short-circuit on simple heuristics to keep it fast
    Returns:
        Tuple of (has_api_changes, details)
    """
    details: Dict[str, object] = {
        "matched": False,
        "checked": False,
        "has_api_keywords": False,
        "total_lines_changed": commit.lines_added + commit.lines_deleted,
        "changed_files": 0,
        "code_files": 0,
        "skipped_reason": None,
        "symbol_diff": False
    }

    try:
        changed_files = get_changed_files(repo, commit.hash)
        details["changed_files"] = len(changed_files)
        if not changed_files:
            details["skipped_reason"] = "no_changed_files"
            return False, details

        # Quick heuristics to avoid heavy work on tiny commits
        message_lower = commit.message.lower()
        api_keywords = ["api", "interface", "signature", "breaking", "deprecate", "rename", "remove"]
        has_api_keywords = any(keyword in message_lower for keyword in api_keywords)
        total_lines_changed = commit.lines_added + commit.lines_deleted
        details["has_api_keywords"] = has_api_keywords

        # Only consider Python/Java files for AST-based check (lightweight scope)
        code_files = [
            f for f in changed_files
            if f.endswith(".py") or f.endswith(".java")
        ]
        details["code_files"] = len(code_files)
        if not code_files:
            details["skipped_reason"] = "no_code_files"
            return False, details

        # Skip tiny commits unless message indicates API changes
        if not has_api_keywords and total_lines_changed < 50 and len(code_files) < 3:
            details["skipped_reason"] = "tiny_commit_without_keywords"
            return False, details

        # Identify parent commit
        git_commit = repo.commit(commit.hash)
        if not git_commit.parents:
            details["skipped_reason"] = "no_parent_commit"
            return False, details
        parent_hash = git_commit.parents[0].hexsha

        details["checked"] = True

        for file_path in code_files:
            lang = "python" if file_path.endswith(".py") else "java"

            current_symbols = _get_public_api_symbols(repo, commit.hash, file_path, lang)
            parent_symbols = _get_public_api_symbols(repo, parent_hash, file_path, lang)

            if _has_symbol_diff(current_symbols, parent_symbols):
                details["symbol_diff"] = True
                details["matched"] = True
                return True, details

        return False, details
        
    except Exception as e:
        logger.warning(f"Error checking API changes for commit {commit.hash[:8]}: {e}")
        details["skipped_reason"] = "error"
        return False, details


def compare_api_symbols_between_commits(
    repo: Repo,
    old_hash: str,
    new_hash: str,
    file_paths: Optional[List[str]] = None
) -> Tuple[bool, Dict[str, object]]:
    details: Dict[str, object] = {
        "matched": False,
        "checked": False,
        "changed_files": 0,
        "code_files": 0,
        "symbol_diff": False,
        "skipped_reason": None
    }
    try:
        paths: List[str] = []
        if file_paths is None:
            diff = get_diff_between_refs(repo, old_hash, new_hash)
            if not diff:
                details["skipped_reason"] = "no_diff"
                return False, details
            for item in diff:
                if item.a_path:
                    paths.append(item.a_path)
                if item.b_path and item.b_path != item.a_path:
                    paths.append(item.b_path)
            paths = list(set(paths))
        else:
            paths = file_paths
        details["changed_files"] = len(paths)
        code_files = [
            p for p in paths if p.endswith(".py") or p.endswith(".java")
        ]
        details["code_files"] = len(code_files)
        if not code_files:
            details["skipped_reason"] = "no_code_files"
            return False, details
        details["checked"] = True
        for file_path in code_files:
            lang = "python" if file_path.endswith(".py") else "java"
            old_symbols = _get_public_api_symbols(repo, old_hash, file_path, lang)
            new_symbols = _get_public_api_symbols(repo, new_hash, file_path, lang)
            if _has_symbol_diff(new_symbols, old_symbols):
                details["symbol_diff"] = True
                details["matched"] = True
                return True, details
        return False, details
    except Exception as e:
        logger.warning(f"Error comparing API symbols between {old_hash[:8]} and {new_hash[:8]}: {e}")
        details["skipped_reason"] = "error"
        return False, details


def _get_public_api_symbols(repo: Repo, commit_hash: str, file_path: str, language: str) -> Dict[str, Dict[Tuple, Tuple]]:
    """
    Extract public API symbols from a file at a specific commit.
    Returns dict with keys: "functions", "classes" mapping to signature dicts.
    """
    blob_hash = _get_blob_hash(repo, commit_hash, file_path)
    if not blob_hash:
        return {"functions": {}, "classes": {}}

    cached = _api_symbol_cache.get(blob_hash)
    if cached:
        return cached

    content = _get_file_content(repo, commit_hash, file_path)
    if content is None:
        return {"functions": {}, "classes": {}}

    symbols = _extract_public_symbols_from_content(content, file_path, language)
    _api_symbol_cache[blob_hash] = symbols
    return symbols


def _get_blob_hash(repo: Repo, commit_hash: str, file_path: str) -> Optional[str]:
    try:
        return repo.git.rev_parse(f"{commit_hash}:{file_path}").strip()
    except Exception:
        return None


def _get_file_content(repo: Repo, commit_hash: str, file_path: str) -> Optional[bytes]:
    try:
        text = repo.git.show(f"{commit_hash}:{file_path}")
        return text.encode("utf-8", errors="ignore")
    except Exception:
        return None


def _extract_public_symbols_from_content(content: bytes, file_path: str, language: str) -> Dict[str, Dict[Tuple, Tuple]]:
    from pipeline.ast_parser import parse_file

    with tempfile.NamedTemporaryFile(suffix=Path(file_path).suffix, delete=True) as tmp:
        tmp.write(content)
        tmp.flush()
        ast_data = parse_file(tmp.name, language)

    if not ast_data:
        return {"functions": {}, "classes": {}}

    public_classes = set()
    class_signatures: Dict[Tuple, Tuple] = {}
    for cls in ast_data.get("classes", []):
        name = cls.get("name")
        if not name:
            continue
        if language == "python" and name.startswith("_"):
            continue
        key = (cls.get("kind"), name)
        signature = (tuple(cls.get("base_classes", [])), tuple(cls.get("implemented_interfaces", [])))
        class_signatures[key] = signature
        public_classes.add(name)

    func_signatures: Dict[Tuple, Tuple] = {}
    for func in ast_data.get("functions", []):
        name = func.get("name")
        if not name:
            continue
        container = func.get("container")

        if language == "python":
            if name.startswith("_"):
                continue
            if container and container.startswith("_"):
                continue
        elif language == "java":
            if func.get("visibility") != "public":
                continue

        key = (func.get("kind"), container, name)
        signature = (tuple(func.get("parameters", [])), func.get("return_type"))
        func_signatures[key] = signature

    return {"functions": func_signatures, "classes": class_signatures}


def _has_symbol_diff(current: Dict[str, Dict[Tuple, Tuple]], previous: Dict[str, Dict[Tuple, Tuple]]) -> bool:
    curr_funcs = current.get("functions", {})
    prev_funcs = previous.get("functions", {})
    curr_classes = current.get("classes", {})
    prev_classes = previous.get("classes", {})

    # Function changes: add/remove/signature change
    if set(curr_funcs.keys()) != set(prev_funcs.keys()):
        return True
    for key, sig in curr_funcs.items():
        if prev_funcs.get(key) != sig:
            return True

    # Class changes: add/remove/base/interface change
    if set(curr_classes.keys()) != set(prev_classes.keys()):
        return True
    for key, sig in curr_classes.items():
        if prev_classes.get(key) != sig:
            return True

    return False


def is_large_refactoring(commit: CommitInfo, repo: Repo, config: SlicingConfig) -> Tuple[bool, Dict[str, object]]:
    """
    Detect large-scale refactoring events.
    
    Args:
        commit: Commit information
        repo: Git repository object
        config: Slicing configuration
        
    Returns:
        Tuple of (is_refactoring, details)
    """
    details: Dict[str, object] = {
        "matched": False,
        "renames": 0,
        "refactor_keywords": False,
        "files_changed": commit.files_changed,
        "threshold_files": config.refactoring_file_threshold
    }

    try:
        # Check for file renames
        renames = detect_file_renames(repo, commit.hash)
        details["renames"] = len(renames)
        if len(renames) >= config.refactoring_file_threshold:
            details["matched"] = True
            return True, details
        
        # Check commit message for refactoring keywords
        message_lower = commit.message.lower()
        refactor_keywords = ["refactor", "restructure", "reorganize", "move", "rename"]
        if any(keyword in message_lower for keyword in refactor_keywords):
            details["refactor_keywords"] = True
            # Check if many files changed
            if commit.files_changed >= config.refactoring_file_threshold:
                details["matched"] = True
                return True, details
        
        return False, details
        
    except Exception as e:
        logger.warning(f"Error checking refactoring for commit {commit.hash[:8]}: {e}")
        return False, details


def create_slice_from_commit(
    commit: CommitInfo,
    repo: Repo,
    slice_type: SliceType,
    config: Config,
    slice_score: float,
    score_breakdown: Dict[str, object]
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
            files_modified=changed_files,
            slice_score=slice_score,
            score_breakdown=score_breakdown
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
    for commit, score, _, _ in scored_commits:
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
