"""Shared datatypes for QA generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class SliceContext:
    repo: str
    slice_id: str
    version_tag: str | None
    commit_hash: str
    commit_date: str
    functions: List[Dict[str, Any]]
    classes: List[Dict[str, Any]]
    imports: List[Dict[str, Any]]
    module_docs: List[Dict[str, Any]]
