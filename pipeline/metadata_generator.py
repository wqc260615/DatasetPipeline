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

from pipeline.models import SemanticSlice, SliceMetadata, CodeFile
from pipeline.config import Config
from pipeline.ast_parser import parse_slice_files, detect_language

logger = logging.getLogger(__name__)


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
        
        # Convert to CodeFile objects
        code_files = []
        total_lines = 0
        
        for parsed_file in parsed_files:
            # parse_file now returns symbol-level data directly
            file_path = parsed_file["file_path"]
            language = parsed_file["language"]
            content_hash = parsed_file["content_hash"]
            functions = parsed_file["functions"]
            classes = parsed_file["classes"]
            comments = parsed_file["comments"]
            
            # Make path relative to repo root
            rel_path = str(Path(file_path).relative_to(repo_path))
            
            # Count lines
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    total_lines += len(lines)
            except Exception:
                pass
            
            code_file = CodeFile(
                path=rel_path,
                content_hash=content_hash,
                functions=functions,
                classes=classes,
                comments=comments,
                language=language
            )
            
            code_files.append(code_file)
        
        # Update slice
        slice.files = code_files
        
        # Update metadata
        slice.metadata.total_files = len(code_files)
        slice.metadata.total_lines = total_lines
        
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
        "total_files": len(slice.files),
        "total_functions": 0,
        "total_classes": 0,
        "languages": {},
        "total_lines": slice.metadata.total_lines
    }
    
    for file in slice.files:
        stats["total_functions"] += len(file.functions)
        stats["total_classes"] += len(file.classes)
        
        lang = file.language or "unknown"
        stats["languages"][lang] = stats["languages"].get(lang, 0) + 1
    
    return stats


def compare_slices(prev_slice: SemanticSlice, curr_slice: SemanticSlice) -> dict:
    """
    Compare two slices to identify changes.
    
    Args:
        prev_slice: Previous slice
        curr_slice: Current slice
        
    Returns:
        Dictionary with comparison results
    """
    prev_files = {f.path: f.content_hash for f in prev_slice.files}
    curr_files = {f.path: f.content_hash for f in curr_slice.files}
    
    added_files = set(curr_files.keys()) - set(prev_files.keys())
    removed_files = set(prev_files.keys()) - set(curr_files.keys())
    modified_files = {
        path for path in set(prev_files.keys()) & set(curr_files.keys())
        if prev_files[path] != curr_files[path]
    }
    
    return {
        "added_files": list(added_files),
        "removed_files": list(removed_files),
        "modified_files": list(modified_files),
        "files_added_count": len(added_files),
        "files_removed_count": len(removed_files),
        "files_modified_count": len(modified_files)
    }
