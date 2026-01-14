"""
Data models for representing repositories, slices, and metadata.

This module defines Pydantic models for type-safe data structures
used throughout the pipeline.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, field_validator


class SliceType(str, Enum):
    """Types of semantic evolution slices."""
    VERSION_RELEASE = "version_release"
    FEATURE = "feature"
    API_CHANGE = "api_change"
    REFACTORING = "refactoring"


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


class CodeFile(BaseModel):
    """Representation of a source code file in a slice (symbol-level only)."""
    path: str = Field(..., description="Relative path from repository root")
    content_hash: str = Field(..., description="SHA256 hash of file content")
    functions: List[Dict[str, Any]] = Field(default_factory=list, description="Extracted function symbols")
    classes: List[Dict[str, Any]] = Field(default_factory=list, description="Extracted class symbols")
    comments: List[Dict[str, Any]] = Field(default_factory=list, description="Extracted comments")
    language: Optional[str] = Field(None, description="Detected programming language")


class SliceMetadata(BaseModel):
    """Metadata for a semantic evolution slice."""
    total_files: int = Field(..., description="Total number of files in slice")
    total_lines: int = Field(..., description="Total lines of code")
    changed_files_since_prev_slice: int = Field(..., description="Number of files changed since previous slice")
    commit_message: str = Field(..., description="Commit message")
    lines_added: Optional[int] = Field(None, description="Lines added in this commit")
    lines_deleted: Optional[int] = Field(None, description="Lines deleted in this commit")
    files_modified: Optional[List[str]] = Field(None, description="List of modified file paths")


class SemanticSlice(BaseModel):
    """A semantic evolution slice representing a meaningful repository state."""
    slice_id: str = Field(..., description="Unique identifier for the slice")
    commit_hash: str = Field(..., description="Git commit hash")
    commit_date: str = Field(..., description="ISO format commit timestamp")
    slice_type: SliceType = Field(..., description="Type of semantic change")
    version_tag: Optional[str] = Field(None, description="Version tag if applicable")
    files: List[CodeFile] = Field(default_factory=list, description="Code files in this slice")
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
    
    def get_slice_by_id(self, slice_id: str) -> Optional[SemanticSlice]:
        """Get a slice by its ID."""
        for slice in self.slices:
            if slice.slice_id == slice_id:
                return slice
        return None
    
    def get_slices_by_type(self, slice_type: SliceType) -> List[SemanticSlice]:
        """Get all slices of a specific type."""
        return [s for s in self.slices if s.slice_type == slice_type]


class CommitInfo(BaseModel):
    """Information about a Git commit."""
    hash: str = Field(..., description="Commit hash")
    message: str = Field(..., description="Commit message")
    author: str = Field(..., description="Author name and email")
    date: datetime = Field(..., description="Commit timestamp")
    files_changed: int = Field(..., description="Number of files changed")
    lines_added: int = Field(..., description="Lines added")
    lines_deleted: int = Field(..., description="Lines deleted")
    is_merge: bool = Field(default=False, description="Whether this is a merge commit")
    tags: List[str] = Field(default_factory=list, description="Git tags pointing to this commit")
