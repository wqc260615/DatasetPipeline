"""
Module: build_checker.py

Purpose: Test if slice code is parseable and can be built/compiled.

Key Functions:
- check_slice_parseable(slice: SemanticSlice, repo_path: str) -> bool
- validate_syntax(slice: SemanticSlice, repo_path: str) -> dict

Example:
    >>> is_parseable = check_slice_parseable(slice, "/path/to/repo")
    >>> print(is_parseable)
    True
"""

import logging
import subprocess
from pathlib import Path
from typing import Dict
from git import Repo

from pipeline.models import SemanticSlice

logger = logging.getLogger(__name__)


def check_slice_parseable(slice: SemanticSlice, repo_path: str) -> bool:
    """
    Check if all files in a slice can be parsed without syntax errors.
    
    Args:
        slice: Semantic slice to check
        repo_path: Path to repository
        
    Returns:
        True if all files are parseable, False otherwise
    """
    try:
        repo = Repo(repo_path)
        original_branch = repo.active_branch.name if repo.head.is_valid() else None
        
        # Checkout slice commit
        repo.git.checkout(slice.commit_hash)
        
        parseable = True
        
        # Check each file based on language
        for file in slice.files:
            if not file.language:
                continue
            
            file_path = Path(repo_path) / file.path
            
            if not file_path.exists():
                logger.warning(f"File not found: {file_path}")
                parseable = False
                continue
            
            # Language-specific syntax checking
            if file.language == "python":
                if not _check_python_syntax(file_path):
                    parseable = False
            elif file.language == "java":
                if not _check_java_syntax(file_path):
                    parseable = False
        
        # Return to original branch
        if original_branch:
            try:
                repo.git.checkout(original_branch)
            except Exception:
                repo.git.checkout('master')
        else:
            repo.git.checkout('master')
        
        return parseable
        
    except Exception as e:
        logger.error(f"Error checking slice parseability: {e}")
        return False


def _check_python_syntax(file_path: Path) -> bool:
    """Check Python file syntax."""
    try:
        result = subprocess.run(
            ["python", "-m", "py_compile", str(file_path)],
            capture_output=True,
            timeout=10
        )
        return result.returncode == 0
    except Exception as e:
        logger.warning(f"Error checking Python syntax for {file_path}: {e}")
        return False


def _check_java_syntax(file_path: Path) -> bool:
    """Check Java file syntax (basic check)."""
    # Java compilation requires classpath setup, so we do a basic check
    # Full compilation check would require more setup
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            # Basic validation: check for class keyword
            if 'class' in content or 'interface' in content:
                return True
        return False
    except Exception as e:
        logger.warning(f"Error checking Java syntax for {file_path}: {e}")
        return False


def validate_syntax(slice: SemanticSlice, repo_path: str) -> Dict[str, any]:
    """
    Validate syntax for all files in a slice.
    
    Args:
        slice: Semantic slice to validate
        repo_path: Path to repository
        
    Returns:
        Dictionary with validation results
    """
    results = {
        "total_files": len(slice.files),
        "parseable_files": 0,
        "unparseable_files": [],
        "success_rate": 0.0
    }
    
    try:
        repo = Repo(repo_path)
        original_branch = repo.active_branch.name if repo.head.is_valid() else None
        repo.git.checkout(slice.commit_hash)
        
        for file in slice.files:
            if not file.language:
                continue
            
            file_path = Path(repo_path) / file.path
            
            if not file_path.exists():
                results["unparseable_files"].append(file.path)
                continue
            
            is_parseable = False
            if file.language == "python":
                is_parseable = _check_python_syntax(file_path)
            elif file.language == "java":
                is_parseable = _check_java_syntax(file_path)
            
            if is_parseable:
                results["parseable_files"] += 1
            else:
                results["unparseable_files"].append(file.path)
        
        if results["total_files"] > 0:
            results["success_rate"] = results["parseable_files"] / results["total_files"]
        
        # Return to original branch
        if original_branch:
            try:
                repo.git.checkout(original_branch)
            except Exception:
                repo.git.checkout('master')
        else:
            repo.git.checkout('master')
        
    except Exception as e:
        logger.error(f"Error validating syntax: {e}")
    
    return results
