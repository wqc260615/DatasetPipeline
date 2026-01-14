"""
Module: output_writer.py

Purpose: Save repository dataset to a structured directory layout.

Key Functions:
- save_repository_dataset(dataset: RepositoryDataset, output_dir: Path) -> None
- generate_summary(dataset: RepositoryDataset) -> dict

Example:
    >>> from pathlib import Path
    >>> save_repository_dataset(dataset, Path("./output"))
"""

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import List, Dict, Any

from pipeline.models import RepositoryDataset, SemanticSlice

logger = logging.getLogger(__name__)


def save_repository_dataset(dataset: RepositoryDataset, output_dir: Path) -> None:
    """
    Save repository dataset to structured directory layout.
    
    Structure:
    output/{repo_name}/
    ├── metadata.json              # Lightweight: repository info + slice metadata only
    ├── slices/
    │   ├── slice_0001/
    │   │   ├── metadata.json      # Slice metadata
    │   │   ├── files.json         # File list and hashes
    │   │   └── symbols/           # Symbol-level data only
    │   │       ├── functions.json
    │   │       ├── classes.json
    │   │       └── comments.json
    │   └── slice_0002/
    │       └── ...
    └── summary.json               # Overall statistics
    
    Args:
        dataset: RepositoryDataset to save
        output_dir: Base output directory
    """
    repo_name = dataset.repository.name.replace('/', '_')
    repo_output_dir = output_dir / repo_name
    slices_dir = repo_output_dir / "slices"
    
    # Create directory structure
    slices_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Saving dataset to: {repo_output_dir}")
    
    # 1. Save metadata.json (lightweight: repository info + slice metadata only)
    metadata = {
        "repository": dataset.repository.model_dump(),
        "slices": [
            {
                "slice_id": slice.slice_id,
                "commit_hash": slice.commit_hash,
                "commit_date": slice.commit_date,
                "slice_type": slice.slice_type.value,
                "version_tag": slice.version_tag,
                "metadata": slice.metadata.model_dump()
            }
            for slice in dataset.slices
        ]
    }
    
    metadata_path = repo_output_dir / "metadata.json"
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, default=str, ensure_ascii=False)
    logger.info(f"Saved metadata.json: {metadata_path}")
    
    # 2. Save each slice's data
    for idx, slice in enumerate(dataset.slices, 1):
        slice_dir = slices_dir / f"slice_{idx:04d}"
        slice_dir.mkdir(parents=True, exist_ok=True)
        
        _save_slice_data(slice, slice_dir)
    
    # 3. Save summary.json
    summary = generate_summary(dataset)
    summary_path = repo_output_dir / "summary.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str, ensure_ascii=False)
    logger.info(f"Saved summary.json: {summary_path}")
    
    logger.info(f"Successfully saved dataset for {repo_name}")


def _save_slice_data(slice: SemanticSlice, slice_dir: Path) -> None:
    """
    Save individual slice data to its directory (symbol-level only).
    
    Args:
        slice: SemanticSlice to save
        slice_dir: Directory for this slice
    """
    # Save metadata.json (slice metadata)
    metadata = {
        "slice_id": slice.slice_id,
        "commit_hash": slice.commit_hash,
        "commit_date": slice.commit_date,
        "slice_type": slice.slice_type.value,
        "version_tag": slice.version_tag,
        "metadata": slice.metadata.model_dump()
    }
    
    metadata_path = slice_dir / "metadata.json"
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, default=str, ensure_ascii=False)
    
    # Save files.json (file list and hashes)
    files_data = [
        {
            "path": file.path,
            "content_hash": file.content_hash,
            "language": file.language
        }
        for file in slice.files
    ]
    
    files_path = slice_dir / "files.json"
    with open(files_path, 'w', encoding='utf-8') as f:
        json.dump(files_data, f, indent=2, default=str, ensure_ascii=False)
    
    # Save symbol-level data to symbols/ directory
    symbols_dir = slice_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    
    # Collect all functions, classes, and comments from all files
    all_functions = []
    all_classes = []
    all_comments = []
    
    for file in slice.files:
        # Add file path to each symbol for traceability
        for func in file.functions:
            func_with_file = func.copy()
            func_with_file["file_path"] = file.path
            all_functions.append(func_with_file)
        
        for cls in file.classes:
            cls_with_file = cls.copy()
            cls_with_file["file_path"] = file.path
            all_classes.append(cls_with_file)
        
        for comment in file.comments:
            comment_with_file = comment.copy()
            comment_with_file["file_path"] = file.path
            all_comments.append(comment_with_file)
    
    # Save functions.json
    functions_path = symbols_dir / "functions.json"
    with open(functions_path, 'w', encoding='utf-8') as f:
        json.dump(all_functions, f, indent=2, default=str, ensure_ascii=False)
    
    # Save classes.json
    classes_path = symbols_dir / "classes.json"
    with open(classes_path, 'w', encoding='utf-8') as f:
        json.dump(all_classes, f, indent=2, default=str, ensure_ascii=False)
    
    # Save comments.json
    comments_path = symbols_dir / "comments.json"
    with open(comments_path, 'w', encoding='utf-8') as f:
        json.dump(all_comments, f, indent=2, default=str, ensure_ascii=False)


def generate_summary(dataset: RepositoryDataset) -> dict:
    """
    Generate overall statistics for the repository dataset.
    
    Args:
        dataset: RepositoryDataset
        
    Returns:
        Dictionary with summary statistics
    """
    from pipeline.metadata_generator import calculate_slice_statistics
    
    total_slices = len(dataset.slices)
    total_files = sum(len(slice.files) for slice in dataset.slices)
    total_lines = sum(slice.metadata.total_lines for slice in dataset.slices)
    
    slice_types = {}
    languages = {}
    total_functions = 0
    total_classes = 0
    
    for slice in dataset.slices:
        slice_type = slice.slice_type.value
        slice_types[slice_type] = slice_types.get(slice_type, 0) + 1
        
        stats = calculate_slice_statistics(slice)
        total_functions += stats["total_functions"]
        total_classes += stats["total_classes"]
        
        for lang, count in stats["languages"].items():
            languages[lang] = languages.get(lang, 0) + count
    
    summary = {
        "repository": dataset.repository.model_dump(),
        "statistics": {
            "total_slices": total_slices,
            "total_files": total_files,
            "total_lines": total_lines,
            "total_functions": total_functions,
            "total_classes": total_classes,
            "slice_types": slice_types,
            "languages": languages
        },
        "slice_ids": [slice.slice_id for slice in dataset.slices]
    }
    
    return summary
