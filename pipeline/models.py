"""
Data models for representing repositories, slices, and metadata.

This module defines Pydantic models for type-safe data structures
used throughout the pipeline.

File-level symbol data uses the QA-enriched models (QACodeFile,
QAFunctionSymbol, QAClassSymbol, QAImport, etc.) which carry richer
metadata than the legacy CodeFile: typed parameters, return types,
decorators, class fields, imports, and module-level docstrings.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, field_validator


class SliceType(str, Enum):
    """Types of semantic evolution slices."""
    VERSION_RELEASE = "version_release"


class RepositoryInfo(BaseModel):
    """Repository metadata."""
    name: str = Field(..., description="Repository name in format owner/repo")
    url: str = Field(..., description="Repository URL")
    language: str = Field(..., description="Primary programming language")
    clone_date: str = Field(..., description="ISO format date when repository was cloned")
    
    @field_validator('clone_date')
    @classmethod
    def validate_date(cls, v: str) -> str:
        """Validate date format."""
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError("Date must be in ISO format")
        return v


# CodeFile has been removed — use QACodeFile instead.
# QACodeFile (defined below) is the single authoritative file-level model
# and carries all data previously in CodeFile plus richer QA metadata.


class SliceMetadata(BaseModel):
    """Metadata for a semantic evolution slice."""
    total_files: int = Field(..., description="Total number of files in repository snapshot")
    total_lines: int = Field(..., description="Total number of lines in repository snapshot")
    target_language_total_files: int = Field(
        default=0,
        description="Total number of target-language files in slice (configured extensions)"
    )
    target_language_total_lines: int = Field(
        default=0,
        description="Total lines in target-language files"
    )
    changed_files_since_prev_slice: int = Field(..., description="Number of files changed since previous slice")
    commit_message: str = Field(..., description="Commit message")
    lines_added: Optional[int] = Field(None, description="Lines added in this commit")
    lines_deleted: Optional[int] = Field(None, description="Lines deleted in this commit")
    files_modified: Optional[List[str]] = Field(None, description="List of modified file paths")
    slice_score: float = Field(0.0, description="Slice score used for selection")
    score_breakdown: Dict[str, Any] = Field(
        default_factory=dict,
        description="Breakdown of how the slice score was computed"
    )


class SemanticSlice(BaseModel):
    """A semantic evolution slice representing a meaningful repository state."""
    slice_id: str = Field(..., description="Unique identifier for the slice")
    commit_hash: str = Field(..., description="Git commit hash")
    commit_date: str = Field(..., description="ISO format commit timestamp")
    slice_type: SliceType = Field(..., description="Type of semantic change")
    version_tag: Optional[str] = Field(None, description="Version tag if applicable")
    files: List["QACodeFile"] = Field(default_factory=list, description="Code files in this slice (QA-enriched)")
    metadata: SliceMetadata = Field(..., description="Slice metadata")
    
    @field_validator('commit_date')
    @classmethod
    def validate_date(cls, v: str) -> str:
        """Validate date format."""
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError("Date must be in ISO format")
        return v


class RepositoryDataset(BaseModel):
    """Complete dataset for a repository with all slices."""
    repository: RepositoryInfo = Field(..., description="Repository information")
    slices: List[SemanticSlice] = Field(default_factory=list, description="Semantic evolution slices")
    
# ============================================================
# QA-specific models (richer metadata for QA pair generation)
# ============================================================


class QAParameter(BaseModel):
    """A function/method parameter with optional type and default value."""
    name: str = Field(..., description="Parameter name")
    type_annotation: Optional[str] = Field(None, description="Type annotation string, e.g. 'int', 'List[str]'")
    default_value: Optional[str] = Field(None, description="Default value as source text, e.g. 'None', '42'")


class QAFunctionSymbol(BaseModel):
    """Enriched function/method symbol for QA generation."""
    name: str = Field(..., description="Function or method name")
    kind: str = Field(..., description="'function', 'method', or 'constructor'")
    container: Optional[str] = Field(None, description="Enclosing class name, if any")
    signature: str = Field(..., description="Full signature line")
    parameters: List[QAParameter] = Field(default_factory=list, description="Parameters with types and defaults")
    return_type: Optional[str] = Field(None, description="Return type annotation")
    decorators: List[str] = Field(default_factory=list, description="Decorator names, e.g. ['staticmethod', 'override']")
    visibility: Optional[str] = Field(None, description="'public', 'protected', 'private'")
    is_static: bool = Field(default=False)
    is_abstract: bool = Field(default=False)
    start_line: int = Field(..., description="1-indexed start line")
    end_line: int = Field(..., description="1-indexed end line")
    doc: Optional[str] = Field(None, description="Associated docstring / Javadoc text")
    file: str = Field(..., description="File path")


class QAFieldSymbol(BaseModel):
    """A class-level field or attribute."""
    name: str = Field(..., description="Field name")
    type_annotation: Optional[str] = Field(None, description="Type annotation")
    default_value: Optional[str] = Field(None, description="Default value as source text")
    visibility: Optional[str] = Field(None, description="'public', 'protected', 'private'")
    is_static: bool = Field(default=False)


class QAClassSymbol(BaseModel):
    """Enriched class/interface symbol for QA generation."""
    name: str = Field(..., description="Class or interface name")
    kind: str = Field(..., description="'class', 'interface', or 'abstract_class'")
    base_classes: List[str] = Field(default_factory=list, description="Superclass names")
    implemented_interfaces: List[str] = Field(default_factory=list, description="Implemented interface names")
    decorators: List[str] = Field(default_factory=list, description="Class-level decorators")
    fields: List[QAFieldSymbol] = Field(default_factory=list, description="Class-level fields/attributes")
    methods: List[str] = Field(default_factory=list, description="Method names defined in this class")
    visibility: Optional[str] = Field(None, description="Java visibility modifier")
    is_abstract: bool = Field(default=False)
    start_line: int = Field(..., description="1-indexed start line")
    end_line: int = Field(..., description="1-indexed end line")
    doc: Optional[str] = Field(None, description="Associated docstring / Javadoc text")
    file: str = Field(..., description="File path")


class QAImport(BaseModel):
    """An import statement."""
    module: str = Field(..., description="Imported module or package")
    names: List[str] = Field(default_factory=list, description="Specific names imported (from X import a, b)")
    alias: Optional[str] = Field(None, description="Import alias")
    is_wildcard: bool = Field(default=False, description="True for 'from X import *'")


class QACodeFile(BaseModel):
    """A source file with QA-enriched metadata.
    
    This is the authoritative file-level model used throughout the pipeline.
    It replaces the former CodeFile and includes all its fields plus richer
    QA data.
    """
    path: str = Field(..., description="Relative path from repository root")
    content_hash: str = Field(..., description="SHA256 hash of file content")
    language: Optional[str] = Field(None, description="Detected programming language")
    module_doc: Optional[str] = Field(None, description="Module-level docstring")
    functions: List[QAFunctionSymbol] = Field(default_factory=list, description="QA function symbols")
    classes: List[QAClassSymbol] = Field(default_factory=list, description="QA class symbols")
    imports: List[QAImport] = Field(default_factory=list, description="Import statements")


# Resolve the forward reference 'QACodeFile' used in SemanticSlice
SemanticSlice.model_rebuild()
