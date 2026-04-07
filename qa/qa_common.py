"""Common helpers for QA generation."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List


def stable_id(*parts: str) -> str:
    payload = "||".join(parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def format_params(params: List[Dict[str, Any]]) -> str:
    if not params:
        return "()"
    chunks: List[str] = []
    for p in params:
        name = p.get("name", "")
        ann = p.get("type_annotation")
        default = p.get("default_value")
        item = name
        if ann:
            item = f"{item}: {ann}"
        if default is not None:
            item = f"{item} = {default}"
        chunks.append(item)
    return f"({', '.join(chunks)})"


def public_function(func: Dict[str, Any]) -> bool:
    visibility = (func.get("visibility") or "").lower()
    if visibility:
        return visibility == "public"
    name = func.get("name", "")
    return not name.startswith("_")


def public_class(cls: Dict[str, Any]) -> bool:
    visibility = cls.get("visibility")
    if visibility is None:
        return True
    return visibility.lower() == "public"


def symbol_ref(symbol: Dict[str, Any]) -> str:
    container = symbol.get("container")
    name = symbol.get("name", "")
    if container:
        return f"{container}.{name}"
    return name


def first_sentence(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    for sep in [". ", "。", "\n"]:
        if sep in text:
            return text.split(sep)[0].strip()
    return text


def make_qa(
    repo: str,
    qa_type: str,
    subtype: str,
    question: str,
    answer: str,
    evidence: Dict[str, Any],
    slice_id: str | None = None,
    from_slice_id: str | None = None,
    to_slice_id: str | None = None,
) -> Dict[str, Any]:
    evidence_key = json.dumps(evidence or {}, ensure_ascii=False, sort_keys=True)
    qa_id = stable_id(
        repo,
        qa_type,
        subtype,
        question,
        answer,
        slice_id or "",
        from_slice_id or "",
        to_slice_id or "",
        evidence_key,
    )
    return {
        "qa_id": qa_id,
        "repo": repo,
        "qa_type": qa_type,
        "qa_subtype": subtype,
        "question": question,
        "answer": answer,
        "slice_id": slice_id,
        "from_slice_id": from_slice_id,
        "to_slice_id": to_slice_id,
        "evidence": evidence,
    }
