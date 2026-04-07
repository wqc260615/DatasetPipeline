"""Generate temporal QA pairs between two slice contexts (pairwise) and across the full slice chain (ordering and evolution).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

from qa.qa_common import format_params, make_qa, public_class, public_function, symbol_ref
from qa.qa_types import SliceContext

# ---------------------------------------------------------------------------
# Test-path filter
# ---------------------------------------------------------------------------

_TEST_PATH_RE = re.compile(
    r"(?:^|[/\\])tests?[/\\]"       # Python: tests/ or test/ directory
    r"|(?:^|[/\\])test_"            # Python: test_foo.py prefix
    r"|_tests?\.py$"                # Python: foo_test.py / foo_tests.py
    r"|[/\\]src[/\\]test[/\\]"      # Java/Maven: src/test/java/
    r"|Test\.java$"                 # Java: FooTest.java
    r"|Tests\.java$",               # Java: FooTests.java
    re.IGNORECASE,
)


def _is_test_path(file_path: str) -> bool:
    return bool(_TEST_PATH_RE.search(file_path or ""))


def _prod_function(func: Dict[str, Any]) -> bool:
    """Public function/method that lives in production (non-test) code."""
    return public_function(func) and not _is_test_path(func.get("file_path") or "")


def _prod_class(cls: Dict[str, Any]) -> bool:
    """Public class that lives in production (non-test) code."""
    return public_class(cls) and not _is_test_path(cls.get("file_path") or "")


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------


def _function_key(func: Dict[str, Any]) -> str:
    return "{}|{}|{}|{}".format(
        func.get("file_path") or "",
        func.get("container") or "",
        func.get("kind") or "",
        func.get("name") or "",
    )


def _class_key(cls: Dict[str, Any]) -> str:
    return "{}|{}|{}".format(
        cls.get("file_path") or "",
        cls.get("kind") or "",
        cls.get("name") or "",
    )


def _question_key(repo: str, subtype: str, ref: str, file_path: str = "") -> str:
    """version-agnostic key linking the same fact across slice transitions."""
    return f"{repo}||temporal||{subtype}||{ref}||{file_path}"


# ---------------------------------------------------------------------------
# deterministic uniform sampling of stable symbol keys
# ---------------------------------------------------------------------------


def _sample_stable(items: list, max_n: int) -> list:
    """Return at most *max_n* items with uniform deterministic stride."""
    if not items or max_n <= 0:
        return []
    if len(items) <= max_n:
        return items
    step = len(items) / max_n
    return [items[int(i * step)] for i in range(max_n)]


# ---------------------------------------------------------------------------
# Pairwise temporal QA builder
# ---------------------------------------------------------------------------


def build_temporal_qas(
    prev_ctx: SliceContext,
    curr_ctx: SliceContext,
    max_stable_per_type: int = 20,
) -> List[Dict[str, Any]]:
    """Build QA pairs comparing two consecutive slices.

    Parameters
    ----------
    prev_ctx, curr_ctx:
        Adjacent slice contexts (prev is earlier).
    max_stable_per_type:
        Maximum number of *negative* / *stable* examples generated per
        symbol type per slice pair (avoids exponential blowup on large repos).
    """
    qas: List[Dict[str, Any]] = []
    prev_v = prev_ctx.version_tag or prev_ctx.slice_id
    curr_v = curr_ctx.version_tag or curr_ctx.slice_id

    # ------------------------------------------------------------------ #
    # Functions                                                            #
    # ------------------------------------------------------------------ #
    prev_funcs = {_function_key(f): f for f in prev_ctx.functions if _prod_function(f)}
    curr_funcs = {_function_key(f): f for f in curr_ctx.functions if _prod_function(f)}

    added_func_keys = sorted(set(curr_funcs) - set(prev_funcs))
    removed_func_keys = sorted(set(prev_funcs) - set(curr_funcs))
    stable_func_keys = sorted(set(prev_funcs) & set(curr_funcs))

    # -- positive "introduced" --
    for key in added_func_keys:
        cf = curr_funcs[key]
        ref = symbol_ref(cf)
        fp = cf.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="function_introduced",
                question=f"Was function/method {ref} introduced between {prev_v} and {curr_v}?",
                answer="Yes",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "function_introduced", ref, fp),
            )
        )

    # -- negative "not introduced" (sampled stable functions) --
    for key in _sample_stable(stable_func_keys, max_stable_per_type):
        cf = curr_funcs[key]
        ref = symbol_ref(cf)
        fp = cf.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="function_not_introduced",
                question=f"Was function/method {ref} introduced between {prev_v} and {curr_v}?",
                answer="No",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "function_not_introduced", ref, fp),
            )
        )

    # -- positive "removed" --
    for key in removed_func_keys:
        pf = prev_funcs[key]
        ref = symbol_ref(pf)
        fp = pf.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="function_removed",
                question=f"Was function/method {ref} removed between {prev_v} and {curr_v}?",
                answer="Yes",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "function_removed", ref, fp),
            )
        )

    # -- negative "not removed" (sampled stable functions) --
    for key in _sample_stable(stable_func_keys, max_stable_per_type):
        pf = prev_funcs[key]
        ref = symbol_ref(pf)
        fp = pf.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="function_not_removed",
                question=f"Was function/method {ref} removed between {prev_v} and {curr_v}?",
                answer="No",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "function_not_removed", ref, fp),
            )
        )

    # -- Changed and stable properties for overlap functions --
    stable_sig_no_change: List[str] = []
    stable_ret_no_change: List[str] = []

    for key in stable_func_keys:
        pf = prev_funcs[key]
        cf = curr_funcs[key]
        ref = symbol_ref(cf)
        fp = cf.get("file_path") or ""

        # Signature
        prev_sig = pf.get("signature") or format_params(pf.get("parameters", []))
        curr_sig = cf.get("signature") or format_params(cf.get("parameters", []))
        if prev_sig != curr_sig:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="function_signature_changed",
                    question=(
                        f"How did the signature of function/method {ref} "
                        f"change between {prev_v} and {curr_v}?"
                    ),
                    answer=f"from: {prev_sig} ; to: {curr_sig}",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": fp},
                    question_key=_question_key(curr_ctx.repo, "function_signature_changed", ref, fp),
                )
            )
        else:
            stable_sig_no_change.append(key)

        # Return type
        prev_ret = pf.get("return_type") or "unknown"
        curr_ret = cf.get("return_type") or "unknown"
        if prev_ret != curr_ret:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="function_return_type_changed",
                    question=(
                        f"Did the return type of function/method {ref} "
                        f"change between {prev_v} and {curr_v}?"
                    ),
                    answer=f"from: {prev_ret} ; to: {curr_ret}",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": fp},
                    question_key=_question_key(curr_ctx.repo, "function_return_type_changed", ref, fp),
                )
            )
        else:
            stable_ret_no_change.append(key)

        # calls set changed
        prev_calls = set(pf.get("calls") or [])
        curr_calls = set(cf.get("calls") or [])
        if prev_calls != curr_calls:
            added_calls = sorted(curr_calls - prev_calls)
            removed_calls = sorted(prev_calls - curr_calls)
            parts: List[str] = []
            if added_calls:
                parts.append(f"added: {', '.join(added_calls)}")
            if removed_calls:
                parts.append(f"removed: {', '.join(removed_calls)}")
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="function_calls_changed",
                    question=(
                        f"Did the set of functions called within {ref} "
                        f"change between {prev_v} and {curr_v}?"
                    ),
                    answer="; ".join(parts),
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": fp},
                    question_key=_question_key(curr_ctx.repo, "function_calls_changed", ref, fp),
                )
            )

        # instantiations set changed
        prev_insts = set(pf.get("instantiations") or [])
        curr_insts = set(cf.get("instantiations") or [])
        if prev_insts != curr_insts:
            added_insts = sorted(curr_insts - prev_insts)
            removed_insts = sorted(prev_insts - curr_insts)
            iparts: List[str] = []
            if added_insts:
                iparts.append(f"added: {', '.join(added_insts)}")
            if removed_insts:
                iparts.append(f"removed: {', '.join(removed_insts)}")
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="function_instantiations_changed",
                    question=(
                        f"Did the set of classes instantiated within {ref} "
                        f"change between {prev_v} and {curr_v}?"
                    ),
                    answer="; ".join(iparts),
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "function", "name": ref, "file_path": fp},
                    question_key=_question_key(curr_ctx.repo, "function_instantiations_changed", ref, fp),
                )
            )

    # stable signature confirmation (sampled)
    for key in _sample_stable(stable_sig_no_change, max_stable_per_type // 2):
        cf = curr_funcs[key]
        ref = symbol_ref(cf)
        fp = cf.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="function_signature_unchanged",
                question=(
                    f"Did the signature of function/method {ref} "
                    f"change between {prev_v} and {curr_v}?"
                ),
                answer="No",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "function_signature_unchanged", ref, fp),
            )
        )

    # stable return type confirmation (sampled)
    for key in _sample_stable(stable_ret_no_change, max_stable_per_type // 2):
        cf = curr_funcs[key]
        ref = symbol_ref(cf)
        fp = cf.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="function_return_type_unchanged",
                question=(
                    f"Did the return type of function/method {ref} "
                    f"change between {prev_v} and {curr_v}?"
                ),
                answer="No",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "function", "name": ref, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "function_return_type_unchanged", ref, fp),
            )
        )

    # ------------------------------------------------------------------ #
    # Classes                                                              #
    # ------------------------------------------------------------------ #
    prev_classes = {_class_key(c): c for c in prev_ctx.classes if _prod_class(c)}
    curr_classes = {_class_key(c): c for c in curr_ctx.classes if _prod_class(c)}

    added_cls_keys = sorted(set(curr_classes) - set(prev_classes))
    removed_cls_keys = sorted(set(prev_classes) - set(curr_classes))
    stable_cls_keys = sorted(set(prev_classes) & set(curr_classes))

    # -- positive "class introduced" --
    for key in added_cls_keys:
        cc = curr_classes[key]
        name = cc.get("name", "")
        fp = cc.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="class_introduced",
                question=f"Was class {name} introduced between {prev_v} and {curr_v}?",
                answer="Yes",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "class_introduced", name, fp),
            )
        )

    # -- negative "class not introduced" (sampled) --
    for key in _sample_stable(stable_cls_keys, max_stable_per_type):
        cc = curr_classes[key]
        name = cc.get("name", "")
        fp = cc.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="class_not_introduced",
                question=f"Was class {name} introduced between {prev_v} and {curr_v}?",
                answer="No",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "class_not_introduced", name, fp),
            )
        )

    # -- positive "class removed" --
    for key in removed_cls_keys:
        pc = prev_classes[key]
        name = pc.get("name", "")
        fp = pc.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="class_removed",
                question=f"Was class {name} removed between {prev_v} and {curr_v}?",
                answer="Yes",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "class_removed", name, fp),
            )
        )

    # -- negative "class not removed" (sampled) --
    for key in _sample_stable(stable_cls_keys, max_stable_per_type):
        pc = prev_classes[key]
        name = pc.get("name", "")
        fp = pc.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="class_not_removed",
                question=f"Was class {name} removed between {prev_v} and {curr_v}?",
                answer="No",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "class_not_removed", name, fp),
            )
        )

    # -- Changed and stable class properties --
    stable_inh_no_change: List[str] = []

    for key in stable_cls_keys:
        pc = prev_classes[key]
        cc = curr_classes[key]
        name = cc.get("name", "")
        fp = cc.get("file_path") or ""

        prev_bases = pc.get("base_classes") or []
        curr_bases = cc.get("base_classes") or []
        if prev_bases != curr_bases:
            qas.append(
                make_qa(
                    repo=curr_ctx.repo,
                    qa_type="temporal",
                    subtype="class_inheritance_changed",
                    question=(
                        f"How did the inheritance of class {name} "
                        f"change between {prev_v} and {curr_v}?"
                    ),
                    answer=f"from: {prev_bases} ; to: {curr_bases}",
                    from_slice_id=prev_ctx.slice_id,
                    to_slice_id=curr_ctx.slice_id,
                    evidence={"kind": "class", "name": name, "file_path": fp},
                    question_key=_question_key(curr_ctx.repo, "class_inheritance_changed", name, fp),
                )
            )
        else:
            stable_inh_no_change.append(key)

    # stable inheritance confirmation (sampled)
    for key in _sample_stable(stable_inh_no_change, max_stable_per_type // 2):
        cc = curr_classes[key]
        name = cc.get("name", "")
        fp = cc.get("file_path") or ""
        qas.append(
            make_qa(
                repo=curr_ctx.repo,
                qa_type="temporal",
                subtype="class_inheritance_unchanged",
                question=(
                    f"Did the inheritance of class {name} "
                    f"change between {prev_v} and {curr_v}?"
                ),
                answer="No",
                from_slice_id=prev_ctx.slice_id,
                to_slice_id=curr_ctx.slice_id,
                evidence={"kind": "class", "name": name, "file_path": fp},
                question_key=_question_key(curr_ctx.repo, "class_inheritance_unchanged", name, fp),
            )
        )

    return qas


# ---------------------------------------------------------------------------
# Ordering QAs (full slice chain required)
# ---------------------------------------------------------------------------


def build_ordering_qas(all_ctxs: List[SliceContext]) -> List[Dict[str, Any]]:
    """Generate first-introduction and last-presence ordering questions.

    These require the full ordered slice list and cannot be derived from
    adjacent-pair comparisons alone.
    """
    if len(all_ctxs) < 2:
        return []

    qas: List[Dict[str, Any]] = []
    repo = all_ctxs[0].repo
    span_from = all_ctxs[0].slice_id
    span_to = all_ctxs[-1].slice_id

    first_ctx_func_keys = {_function_key(f) for f in all_ctxs[0].functions if _prod_function(f)}
    last_ctx_func_keys = {_function_key(f) for f in all_ctxs[-1].functions if _prod_function(f)}

    func_first: Dict[str, SliceContext] = {}
    func_last: Dict[str, SliceContext] = {}
    func_repr: Dict[str, Dict[str, Any]] = {}

    for ctx in all_ctxs:
        for f in ctx.functions:
            if not _prod_function(f):
                continue
            key = _function_key(f)
            if key not in func_first:
                func_first[key] = ctx
                func_repr[key] = f
            func_last[key] = ctx

    for key in sorted(func_first):
        f = func_repr[key]
        ref = symbol_ref(f)
        fp = f.get("file_path") or ""

        # "first introduced" — only when the function was absent in slice 0
        if key not in first_ctx_func_keys:
            first_v = func_first[key].version_tag or func_first[key].slice_id
            qas.append(
                make_qa(
                    repo=repo,
                    qa_type="temporal",
                    subtype="function_first_introduced",
                    question=f"In which version was function/method {ref} first introduced?",
                    answer=first_v,
                    from_slice_id=span_from,
                    to_slice_id=span_to,
                    evidence={"kind": "function", "name": ref, "file_path": fp},
                    question_key=_question_key(repo, "function_first_introduced", ref, fp),
                )
            )

        # "last present" — only when the function is absent from the final slice
        if key not in last_ctx_func_keys:
            last_v = func_last[key].version_tag or func_last[key].slice_id
            qas.append(
                make_qa(
                    repo=repo,
                    qa_type="temporal",
                    subtype="function_last_present",
                    question=f"In which version was function/method {ref} last present?",
                    answer=last_v,
                    from_slice_id=span_from,
                    to_slice_id=span_to,
                    evidence={"kind": "function", "name": ref, "file_path": fp},
                    question_key=_question_key(repo, "function_last_present", ref, fp),
                )
            )

    # Classes
    first_ctx_cls_keys = {_class_key(c) for c in all_ctxs[0].classes if _prod_class(c)}
    last_ctx_cls_keys = {_class_key(c) for c in all_ctxs[-1].classes if _prod_class(c)}

    cls_first: Dict[str, SliceContext] = {}
    cls_last: Dict[str, SliceContext] = {}
    cls_repr: Dict[str, Dict[str, Any]] = {}

    for ctx in all_ctxs:
        for c in ctx.classes:
            if not _prod_class(c):
                continue
            key = _class_key(c)
            if key not in cls_first:
                cls_first[key] = ctx
                cls_repr[key] = c
            cls_last[key] = ctx

    for key in sorted(cls_first):
        c = cls_repr[key]
        name = c.get("name", "")
        fp = c.get("file_path") or ""

        if key not in first_ctx_cls_keys:
            first_v = cls_first[key].version_tag or cls_first[key].slice_id
            qas.append(
                make_qa(
                    repo=repo,
                    qa_type="temporal",
                    subtype="class_first_introduced",
                    question=f"In which version was class {name} first introduced?",
                    answer=first_v,
                    from_slice_id=span_from,
                    to_slice_id=span_to,
                    evidence={"kind": "class", "name": name, "file_path": fp},
                    question_key=_question_key(repo, "class_first_introduced", name, fp),
                )
            )

        if key not in last_ctx_cls_keys:
            last_v = cls_last[key].version_tag or cls_last[key].slice_id
            qas.append(
                make_qa(
                    repo=repo,
                    qa_type="temporal",
                    subtype="class_last_present",
                    question=f"In which version was class {name} last present?",
                    answer=last_v,
                    from_slice_id=span_from,
                    to_slice_id=span_to,
                    evidence={"kind": "class", "name": name, "file_path": fp},
                    question_key=_question_key(repo, "class_last_present", name, fp),
                )
            )

    return qas


# ---------------------------------------------------------------------------
# Evolution QAs (full trajectory across all slices)
# ---------------------------------------------------------------------------


def build_evolution_qas(all_ctxs: List[SliceContext]) -> List[Dict[str, Any]]:
    """Generate Scenario-3-style trajectory questions spanning the full range.

    Only emitted when there are ≥ 2 distinct values (i.e. actual evolution
    occurred), so these are never trivially answerable from a single slice.
    """
    if len(all_ctxs) < 3:
        return []

    qas: List[Dict[str, Any]] = []
    repo = all_ctxs[0].repo
    span_from = all_ctxs[0].slice_id
    span_to = all_ctxs[-1].slice_id
    first_v = all_ctxs[0].version_tag or all_ctxs[0].slice_id
    last_v = all_ctxs[-1].version_tag or all_ctxs[-1].slice_id

    func_repr: Dict[str, Dict[str, Any]] = {}
    func_ret_traj: Dict[str, List[tuple]] = {}   # key -> [(version, ret_type)]
    func_sig_traj: Dict[str, List[tuple]] = {}   # key -> [(version, sig)]

    for ctx in all_ctxs:
        v = ctx.version_tag or ctx.slice_id
        for f in ctx.functions:
            if not _prod_function(f):
                continue
            key = _function_key(f)
            if key not in func_repr:
                func_repr[key] = f

            ret = f.get("return_type") or "unknown"
            ret_traj = func_ret_traj.setdefault(key, [])
            if not ret_traj or ret_traj[-1][1] != ret:
                ret_traj.append((v, ret))

            sig = f.get("signature") or format_params(f.get("parameters", []))
            sig_traj = func_sig_traj.setdefault(key, [])
            if not sig_traj or sig_traj[-1][1] != sig:
                sig_traj.append((v, sig))

    for key in sorted(func_ret_traj):
        traj = func_ret_traj[key]
        if len(traj) < 2:
            continue
        f = func_repr[key]
        ref = symbol_ref(f)
        fp = f.get("file_path") or ""
        answer = " -> ".join(f"{v}: {r}" for v, r in traj)
        qas.append(
            make_qa(
                repo=repo,
                qa_type="temporal",
                subtype="function_return_type_evolution",
                question=(
                    f"How did the return type of function/method {ref} "
                    f"evolve from {first_v} to {last_v}?"
                ),
                answer=answer,
                from_slice_id=span_from,
                to_slice_id=span_to,
                evidence={"kind": "function", "name": ref, "file_path": fp},
                question_key=_question_key(repo, "function_return_type_evolution", ref, fp),
            )
        )

    for key in sorted(func_sig_traj):
        traj = func_sig_traj[key]
        if len(traj) < 2:
            continue
        f = func_repr[key]
        ref = symbol_ref(f)
        fp = f.get("file_path") or ""
        answer = " -> ".join(f"{v}: {s}" for v, s in traj)
        qas.append(
            make_qa(
                repo=repo,
                qa_type="temporal",
                subtype="function_signature_evolution",
                question=(
                    f"How did the signature of function/method {ref} "
                    f"evolve from {first_v} to {last_v}?"
                ),
                answer=answer,
                from_slice_id=span_from,
                to_slice_id=span_to,
                evidence={"kind": "function", "name": ref, "file_path": fp},
                question_key=_question_key(repo, "function_signature_evolution", ref, fp),
            )
        )

    # Class inheritance evolution
    cls_repr: Dict[str, Dict[str, Any]] = {}
    cls_inh_traj: Dict[str, List[tuple]] = {}

    for ctx in all_ctxs:
        v = ctx.version_tag or ctx.slice_id
        for c in ctx.classes:
            if not _prod_class(c):
                continue
            key = _class_key(c)
            if key not in cls_repr:
                cls_repr[key] = c
            bases = str(sorted(c.get("base_classes") or []))
            traj = cls_inh_traj.setdefault(key, [])
            if not traj or traj[-1][1] != bases:
                traj.append((v, bases))

    for key in sorted(cls_inh_traj):
        traj = cls_inh_traj[key]
        if len(traj) < 2:
            continue
        c = cls_repr[key]
        name = c.get("name", "")
        fp = c.get("file_path") or ""
        answer = " -> ".join(f"{v}: {b}" for v, b in traj)
        qas.append(
            make_qa(
                repo=repo,
                qa_type="temporal",
                subtype="class_inheritance_evolution",
                question=(
                    f"How did the inheritance of class {name} "
                    f"evolve from {first_v} to {last_v}?"
                ),
                answer=answer,
                from_slice_id=span_from,
                to_slice_id=span_to,
                evidence={"kind": "class", "name": name, "file_path": fp},
                question_key=_question_key(repo, "class_inheritance_evolution", name, fp),
            )
        )

    return qas
