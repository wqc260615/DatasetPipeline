"""
Module: slice_validator.py

Purpose: Verify slice coherence and quality.

Key Functions:
- validate_slice(slice: SemanticSlice) -> bool
- validate_slice_temporal_consistency(slices: List[SemanticSlice]) -> bool
- check_slice_quality(slice: SemanticSlice, config: ValidationConfig) -> dict

Example:
    >>> is_valid = validate_slice(slice)
    >>> print(is_valid)
    True
"""

import logging
from datetime import datetime
from typing import List, Dict
from pathlib import Path

from pipeline.models import SemanticSlice
from pipeline.config import ValidationConfig

logger = logging.getLogger(__name__)


def validate_slice(slice: SemanticSlice) -> bool:
    """
    Validate a single slice for basic requirements.
    
    Args:
        slice: Semantic slice to validate
        
    Returns:
        True if valid, False otherwise
    """
    # Check required fields
    if not slice.slice_id or not slice.commit_hash:
        logger.warning(f"Slice missing required fields: {slice.slice_id}")
        return False
    
    # Validate date format
    try:
        datetime.fromisoformat(slice.commit_date.replace('Z', '+00:00'))
    except ValueError:
        logger.warning(f"Slice has invalid date format: {slice.slice_id}")
        return False
    
    # Check slice ID format
    if len(slice.slice_id) < 5:
        logger.warning(f"Slice ID too short: {slice.slice_id}")
        return False
    
    return True


def validate_slice_temporal_consistency(slices: List[SemanticSlice]) -> bool:
    """
    Validate that slices are temporally consistent (no duplicates, ordered by date).
    
    Args:
        slices: List of slices to validate
        
    Returns:
        True if consistent, False otherwise
    """
    if not slices:
        return True
    
    # Check for duplicate commit hashes
    commit_hashes = [s.commit_hash for s in slices]
    if len(commit_hashes) != len(set(commit_hashes)):
        logger.error("Duplicate commit hashes found in slices")
        return False
    
    # Check temporal ordering
    dates = []
    for slice in slices:
        try:
            date = datetime.fromisoformat(slice.commit_date.replace('Z', '+00:00'))
            dates.append((slice.slice_id, date))
        except ValueError:
            logger.warning(f"Invalid date in slice: {slice.slice_id}")
            return False
    
    # Check if dates are in order
    for i in range(1, len(dates)):
        if dates[i][1] < dates[i-1][1]:
            logger.error(f"Slices not in temporal order: {dates[i-1][0]} > {dates[i][0]}")
            return False
    
    return True


def check_slice_quality(
    slice: SemanticSlice,
    config: ValidationConfig
) -> Dict[str, any]:
    """
    Perform quality checks on a slice.
    
    Args:
        slice: Semantic slice to check
        config: Validation configuration
        
    Returns:
        Dictionary with quality metrics
    """
    quality = {
        "valid": True,
        "issues": [],
        "code_file_count": len([f for f in slice.files if f.language]),
        "total_files": len(slice.files),
        "has_ast_data": False,
        "parsing_success_rate": 0.0
    }
    
    # Check minimum code files
    if quality["code_file_count"] < config.min_code_files_per_slice:
        quality["valid"] = False
        quality["issues"].append(
            f"Too few code files: {quality['code_file_count']} < {config.min_code_files_per_slice}"
        )
    
    # Check symbol parsing success (we store symbol-level data, not full AST)
    # A file is considered successfully parsed if it has language detected and
    # has symbols, imports, or module-level documentation.
    files_with_symbols = sum(
        1 for f in slice.files 
        if f.language and (
            len(f.functions) > 0
            or len(f.classes) > 0
            or len(f.imports) > 0
            or bool(f.module_doc)
        )
    )
    # For code files, also count files that have language but no symbols (might be empty or only comments)
    code_files = [f for f in slice.files if f.language]
    if code_files:
        quality["parsing_success_rate"] = files_with_symbols / len(code_files) if code_files else 0.0
        quality["has_ast_data"] = files_with_symbols > 0
        
        if quality["parsing_success_rate"] < config.ast_parsing_success_rate_threshold:
            quality["valid"] = False
            quality["issues"].append(
                f"Low symbol parsing success rate: {quality['parsing_success_rate']:.2%} < "
                f"{config.ast_parsing_success_rate_threshold:.2%}"
            )
    
    return quality


def validate_all_slices(
    slices: List[SemanticSlice],
    config: ValidationConfig
) -> Dict[str, any]:
    """
    Validate all slices and return summary.
    
    Args:
        slices: List of slices to validate
        config: Validation configuration
        
    Returns:
        Dictionary with validation results
    """
    results = {
        "total_slices": len(slices),
        "valid_slices": 0,
        "invalid_slices": 0,
        "temporal_consistent": False,
        "quality_issues": []
    }
    
    # Check temporal consistency
    results["temporal_consistent"] = validate_slice_temporal_consistency(slices)
    
    # Check each slice
    for slice in slices:
        if validate_slice(slice):
            quality = check_slice_quality(slice, config)
            if quality["valid"]:
                results["valid_slices"] += 1
            else:
                results["invalid_slices"] += 1
                results["quality_issues"].append({
                    "slice_id": slice.slice_id,
                    "issues": quality["issues"]
                })
        else:
            results["invalid_slices"] += 1
    
    return results
