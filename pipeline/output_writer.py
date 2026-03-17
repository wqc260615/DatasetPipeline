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

import json
import logging
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
    │   │   └── symbols/           # QA-enriched symbol data
    │   │       ├── functions.json  # Typed params, decorators, doc, ...
    │   │       ├── classes.json    # Fields, method list, doc, ...
    │   │       ├── imports.json    # Import statements
    │   │       └── module_docs.json # Module-level docstrings
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
    Save individual slice data to its directory (QA-enriched symbol data).

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
    
    # Save files.json (file list, hashes, language)
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
    
    # Save QA-enriched symbol data to symbols/ directory
    symbols_dir = slice_dir / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)
    
    all_functions = []
    all_classes = []
    all_imports = []
    module_docs = []
    
    for file in slice.files:
        rel_path = file.path
        
        # Functions: use model_dump() to flatten Pydantic objects to plain dicts
        for func in file.functions:
            func_dict = func.model_dump()
            func_dict["file_path"] = rel_path
            all_functions.append(func_dict)
        
        # Classes
        for cls in file.classes:
            cls_dict = cls.model_dump()
            cls_dict["file_path"] = rel_path
            all_classes.append(cls_dict)
        
        # Imports
        for imp in file.imports:
            imp_dict = imp.model_dump()
            imp_dict["file_path"] = rel_path
            all_imports.append(imp_dict)
        
        # Module-level docstrings
        if file.module_doc:
            module_docs.append({
                "file_path": rel_path,
                "doc": file.module_doc
            })
    
    # Write JSON files
    functions_path = symbols_dir / "functions.json"
    with open(functions_path, 'w', encoding='utf-8') as f:
        json.dump(all_functions, f, indent=2, default=str, ensure_ascii=False)
    
    classes_path = symbols_dir / "classes.json"
    with open(classes_path, 'w', encoding='utf-8') as f:
        json.dump(all_classes, f, indent=2, default=str, ensure_ascii=False)
    
    imports_path = symbols_dir / "imports.json"
    with open(imports_path, 'w', encoding='utf-8') as f:
        json.dump(all_imports, f, indent=2, default=str, ensure_ascii=False)
    
    module_docs_path = symbols_dir / "module_docs.json"
    with open(module_docs_path, 'w', encoding='utf-8') as f:
        json.dump(module_docs, f, indent=2, default=str, ensure_ascii=False)


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
    total_files = sum(slice.metadata.total_files for slice in dataset.slices)
    total_lines = sum(slice.metadata.total_lines for slice in dataset.slices)
    target_language_total_files = sum(len(slice.files) for slice in dataset.slices)
    target_language_total_lines = sum(
        slice.metadata.target_language_total_lines for slice in dataset.slices
    )
    
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
            "target_language_total_files": target_language_total_files,
            "target_language_total_lines": target_language_total_lines,
            "total_functions": total_functions,
            "total_classes": total_classes,
            "slice_types": slice_types,
            "languages": languages
        },
        "slice_ids": [slice.slice_id for slice in dataset.slices]
    }
    
    return summary
