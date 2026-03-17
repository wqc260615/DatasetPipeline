"""
Module: metadata_generator.py

Purpose: Generate slice-level metadata and enrich slice information.

Key Functions:
- generate_slice_metadata(slice: SemanticSlice, repo: Repo) -> SliceMetadata
- enrich_slice_with_files(slice: SemanticSlice, repo_path: str, config: Config) -> SemanticSlice

Example:
    >>> slice = enrich_slice_with_files(slice, "/path/to/repo", config)
    >>> print(slice.metadata.total_files)
    42
"""

import logging
from pathlib import Path
from typing import List
from git import Repo

from pipeline.models import SemanticSlice, SliceMetadata, QACodeFile, QAFunctionSymbol, QAClassSymbol, QAImport
from pipeline.config import Config
from pipeline.ast_parser import parse_slice_files, detect_language

logger = logging.getLogger(__name__)


def _calculate_repository_totals(repo_path: str) -> tuple[int, int]:
    """
    Calculate repository-wide totals for current checked out snapshot.

    Counts all regular files under repo root (excluding `.git`) and counts
    text lines using utf-8 decoding with ignore errors.

    Args:
        repo_path: Path to repository root

    Returns:
        Tuple of (total_files, total_lines)
    """
    repo_root = Path(repo_path)
    total_files = 0
    total_lines = 0

    for path in repo_root.rglob("*"):
        if not path.is_file():
            continue
        if ".git" in path.parts:
            continue

        total_files += 1

        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                total_lines += sum(1 for _ in f)
        except Exception:
            # Keep file count even if line counting fails
            continue

    return total_files, total_lines


def enrich_slice_with_files(
    slice: SemanticSlice,
    repo_path: str,
    config: Config
) -> SemanticSlice:
    """
    Enrich a slice with parsed file information.
    
    Args:
        slice: Semantic slice to enrich
        repo_path: Path to repository
        config: Configuration object
        
    Returns:
        Enriched SemanticSlice
    """
    logger.info(f"Enriching slice {slice.slice_id} with file information")
    
    try:
        repo = Repo(repo_path)
        
        # Checkout the commit
        original_branch = repo.active_branch.name if repo.head.is_valid() else None
        repo.git.checkout(slice.commit_hash)
        
        # Parse files in this slice
        parsed_files = parse_slice_files(
            repo_path,
            slice.commit_hash,
            config.parsing.supported_extensions,
            config.parsing.timeout_seconds
        )
        
        # Repository-wide totals (all files in snapshot)
        repo_total_files, repo_total_lines = _calculate_repository_totals(repo_path)

        # Convert parsed target-language files to QACodeFile objects
        code_files = []
        target_language_total_lines = 0
        
        for parsed_file in parsed_files:
            # parse_file() now returns QA-enriched data from parse_file_for_qa()
            file_path = parsed_file["file_path"]
            language = parsed_file["language"]
            content_hash = parsed_file["content_hash"]
            module_doc = parsed_file.get("module_doc")
            
            # Build typed model instances for functions
            functions = [
                QAFunctionSymbol(**f) for f in parsed_file.get("functions", [])
            ]
            
            # Build typed model instances for classes
            classes = [
                QAClassSymbol(**c) for c in parsed_file.get("classes", [])
            ]
            
            # Build typed model instances for imports
            imports = [
                QAImport(**i) for i in parsed_file.get("imports", [])
            ]
            
            # Make path relative to repo root
            rel_path = str(Path(file_path).relative_to(repo_path))
            
            # Count lines
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    target_language_total_lines += len(lines)
            except Exception:
                pass
            
            code_file = QACodeFile(
                path=rel_path,
                content_hash=content_hash,
                language=language,
                module_doc=module_doc,
                functions=functions,
                classes=classes,
                imports=imports,
            )
            
            code_files.append(code_file)
        
        # Update slice
        slice.files = code_files
        
        # Update metadata
        # total_* = repository-wide totals
        # target_language_total_* = parsed language totals (e.g. python/java)
        slice.metadata.total_files = repo_total_files
        slice.metadata.total_lines = repo_total_lines
        slice.metadata.target_language_total_files = len(code_files)
        slice.metadata.target_language_total_lines = target_language_total_lines
        
        # Return to original branch
        if original_branch:
            try:
                repo.git.checkout(original_branch)
            except Exception:
                repo.git.checkout('master')  # Fallback
        else:
            repo.git.checkout('master')
        
        logger.info(f"Enriched slice {slice.slice_id} with {len(code_files)} files")
        return slice
        
    except Exception as e:
        logger.error(f"Error enriching slice {slice.slice_id}: {e}")
        return slice


def calculate_slice_statistics(slice: SemanticSlice) -> dict:
    """
    Calculate statistics for a slice.
    
    Args:
        slice: Semantic slice
        
    Returns:
        Dictionary with statistics
    """
    stats = {
        "total_files": slice.metadata.total_files,
        "target_language_total_files": len(slice.files),
        "total_functions": 0,
        "total_classes": 0,
        "languages": {},
        "total_lines": slice.metadata.total_lines,
        "target_language_total_lines": slice.metadata.target_language_total_lines
    }
    
    for file in slice.files:
        stats["total_functions"] += len(file.functions)
        stats["total_classes"] += len(file.classes)
        
        lang = file.language or "unknown"
        stats["languages"][lang] = stats["languages"].get(lang, 0) + 1
    
    return stats
