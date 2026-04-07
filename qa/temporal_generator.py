"""Generate temporal QA pairs between two slice contexts."""

from __future__ import annotations

from typing import Any, Dict, List

from qa.qa_common import format_params, make_qa, public_class, public_function, symbol_ref
from qa.qa_types import SliceContext


def _function_key(func: Dict[str, Any]) -> str:
    file_path = func.get("file_path") or ""
    container = func.get("container") or ""
    name = func.get("name") or ""
    kind = func.get("kind") or ""
    return f"{file_path}|{container}|{kind}|{name}"


def _class_key(cls: Dict[str, Any]) -> str:
    file_path = cls.get("file_path") or ""
    name = cls.get("name") or ""
    kind = cls.get("kind") or ""
    return f"{file_path}|{kind}|{name}"


def build_temporal_qas(prev_ctx: SliceContext, curr_ctx: SliceContext) -> List[Dict[str, Any]]:
    qas: List[Dict[str, Any]] = []

    prev_funcs = {_function_key(f): f for f in prev_ctx.functions if public_function(f)}
    curr_funcs = {_function_key(f): f for f in curr_ctx.functions if public_function(f)}

    all_func_keys = sorted(set(prev_funcs) | set(curr_funcs))
    for key in all_func_keys:
        pf = prev_funcs.get(key)
        cf = curr_funcs.get(key)
        ref = symbol_ref(cf or pf or {})

        if pf is None and cf is not None:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="function_introduced",
                    question=(
                        f"Was function/method {ref} introduced between "
                        f"{prev_ctx.version_tag or prev_ctx.slice_id} and "
                        f"{curr_ctx.version_tag or curr_ctx.slice_id}?"
                    ),
                    answer="Yes",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": cf.get("file_path")},
                )
            )
            continue

        if pf is not None and cf is None:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="function_removed",
                    question=(
                        f"Was function/method {ref} removed between "
                        f"{prev_ctx.version_tag or prev_ctx.slice_id} and "
                        f"{curr_ctx.version_tag or curr_ctx.slice_id}?"
                    ),
                    answer="Yes",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": pf.get("file_path")},
                )
            )
            continue

        prev_sig = pf.get("signature") or format_params(pf.get("parameters", []))
        curr_sig = cf.get("signature") or format_params(cf.get("parameters", []))
        if prev_sig != curr_sig:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="function_signature_changed",
                    question=(
                        f"How did the signature of function/method {ref} change between "
                        f"{prev_ctx.version_tag or prev_ctx.slice_id} and "
                        f"{curr_ctx.version_tag or curr_ctx.slice_id}?"
                    ),
                    answer=f"from: {prev_sig} ; to: {curr_sig}",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": cf.get("file_path")},
                )
            )

        prev_ret = pf.get("return_type") or "unknown"
        curr_ret = cf.get("return_type") or "unknown"
        if prev_ret != curr_ret:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="function_return_type_changed",
                    question=(
                        f"Did the return type of function/method {ref} change between "
                        f"{prev_ctx.version_tag or prev_ctx.slice_id} and "
                        f"{curr_ctx.version_tag or curr_ctx.slice_id}?"
                    ),
                    answer=f"from: {prev_ret} ; to: {curr_ret}",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": cf.get("file_path")},
                )
            )

    prev_classes = {_class_key(c): c for c in prev_ctx.classes if public_class(c)}
    curr_classes = {_class_key(c): c for c in curr_ctx.classes if public_class(c)}

    all_class_keys = sorted(set(prev_classes) | set(curr_classes))
    for key in all_class_keys:
        pc = prev_classes.get(key)
        cc = curr_classes.get(key)
        name = (cc or pc or {}).get("name", "")

        if pc is None and cc is not None:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="class_introduced",
                    question=(
                        f"Was class {name} introduced between "
                        f"{prev_ctx.version_tag or prev_ctx.slice_id} and "
                        f"{curr_ctx.version_tag or curr_ctx.slice_id}?"
                    ),
                    answer="Yes",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "class", "name": name, "file_path": cc.get("file_path")},
                )
            )
            continue

        if pc is not None and cc is None:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="class_removed",
                    question=(
                        f"Was class {name} removed between "
                        f"{prev_ctx.version_tag or prev_ctx.slice_id} and "
                        f"{curr_ctx.version_tag or curr_ctx.slice_id}?"
                    ),
                    answer="Yes",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "class", "name": name, "file_path": pc.get("file_path")},
                )
            )
            continue

        prev_bases = pc.get("base_classes") or []
        curr_bases = cc.get("base_classes") or []
        if prev_bases != curr_bases:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="class_inheritance_changed",
                    question=(
                        f"How did the inheritance of class {name} change between "
                        f"{prev_ctx.version_tag or prev_ctx.slice_id} and "
                        f"{curr_ctx.version_tag or curr_ctx.slice_id}?"
                    ),
                    answer=f"from: {prev_bases} ; to: {curr_bases}",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "class", "name": name, "file_path": cc.get("file_path")},
                )
            )

    return qas
