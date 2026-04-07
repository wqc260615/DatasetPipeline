"""Generate QA pairs from precomputed slice symbol metadata."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from qa.extrinsic_generator import build_extrinsic_qas
from qa.intrinsic_generator import build_intrinsic_qas
from qa.qa_common import public_class, public_function, symbol_ref
from qa.qa_types import SliceContext
from qa.temporal_generator import build_evolution_qas, build_ordering_qas, build_temporal_qas


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_slice_context(repo_name: str, slice_dir: Path) -> SliceContext:
    metadata = _load_json(slice_dir / "metadata.json", {})
    symbols_dir = slice_dir / "symbols"
    return SliceContext(
        repo=repo_name,
        slice_id=metadata.get("slice_id") or slice_dir.name,
        version_tag=metadata.get("version_tag"),
        commit_hash=metadata.get("commit_hash") or "",
        commit_date=metadata.get("commit_date") or "",
        functions=_load_json(symbols_dir / "functions.json", []),
        classes=_load_json(symbols_dir / "classes.json", []),
        imports=_load_json(symbols_dir / "imports.json", []),
        module_docs=_load_json(symbols_dir / "module_docs.json", []),
    )


def _iter_repo_dirs(slices_root: Path, repos: Iterable[str] | None) -> Iterable[Path]:
    selected = set(repos or [])
    for repo_dir in sorted(p for p in slices_root.iterdir() if p.is_dir()):
        if selected and repo_dir.name not in selected:
            continue
        yield repo_dir


def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def generate_qa_dataset(
    slices_root: Path,
    output_dir: Path,
    repos: List[str] | None = None,
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    overall_summary: Dict[str, Any] = {
        "repos": {},
        "total_qa": 0,
        "total_intrinsic": 0,
        "total_extrinsic": 0,
        "total_temporal": 0,
    }

    for repo_dir in _iter_repo_dirs(slices_root, repos):
        repo_name = repo_dir.name
        slice_base = repo_dir / "slices"
        if not slice_base.exists():
            continue

        slice_dirs = sorted(p for p in slice_base.iterdir() if p.is_dir() and p.name.startswith("slice_"))
        if not slice_dirs:
            continue

        contexts = [_load_slice_context(repo_name, d) for d in slice_dirs]

        # Collect all public symbol refs across every slice for negative sampling.
        all_func_refs: set[str] = set()
        all_class_names: set[str] = set()
        for ctx in contexts:
            for f in ctx.functions:
                if public_function(f):
                    all_func_refs.add(symbol_ref(f))
            for c in ctx.classes:
                if public_class(c):
                    name = c.get("name", "")
                    if name:
                        all_class_names.add(name)

        intrinsic_qas: List[Dict[str, Any]] = []
        extrinsic_qas: List[Dict[str, Any]] = []
        temporal_qas: List[Dict[str, Any]] = []
        intrinsic_n = 0
        extrinsic_n = 0
        temporal_n = 0

        for ctx in contexts:
            ctx_func_refs = {symbol_ref(f) for f in ctx.functions if public_function(f)}
            ctx_class_names = {c.get("name", "") for c in ctx.classes if public_class(c)}

            # Deterministic negative samples: sorted complement, capped at 10% of positives.
            neg_funcs = sorted(all_func_refs - ctx_func_refs)
            neg_classes = sorted(all_class_names - ctx_class_names)
            n_neg_funcs = min(len(neg_funcs), max(1, len(ctx_func_refs) // 10))
            n_neg_classes = min(len(neg_classes), max(1, len(ctx_class_names) // 10))
            neg_funcs = neg_funcs[:n_neg_funcs]
            neg_classes = neg_classes[:n_neg_classes]

            i_qas = build_intrinsic_qas(ctx, neg_func_refs=neg_funcs, neg_class_names=neg_classes)
            e_qas = build_extrinsic_qas(ctx)
            intrinsic_qas.extend(i_qas)
            extrinsic_qas.extend(e_qas)
            intrinsic_n += len(i_qas)
            extrinsic_n += len(e_qas)

        for i in range(1, len(contexts)):
            t_qas = build_temporal_qas(contexts[i - 1], contexts[i])
            temporal_qas.extend(t_qas)
            temporal_n += len(t_qas)

        ordering_qas = build_ordering_qas(contexts)
        evolution_qas = build_evolution_qas(contexts)
        temporal_qas.extend(ordering_qas)
        temporal_qas.extend(evolution_qas)
        temporal_n += len(ordering_qas) + len(evolution_qas)

        repo_qas = intrinsic_qas + extrinsic_qas + temporal_qas

        repo_out = output_dir / repo_name
        repo_out.mkdir(parents=True, exist_ok=True)

        intrinsic_jsonl = repo_out / "intrinsic_qa_pairs.jsonl"
        extrinsic_jsonl = repo_out / "extrinsic_qa_pairs.jsonl"
        temporal_jsonl = repo_out / "temporal_qa_pairs.jsonl"
        qa_jsonl = repo_out / "qa_pairs.jsonl"

        _write_jsonl(intrinsic_jsonl, intrinsic_qas)
        _write_jsonl(extrinsic_jsonl, extrinsic_qas)
        _write_jsonl(temporal_jsonl, temporal_qas)
        _write_jsonl(qa_jsonl, repo_qas)

        summary = {
            "repo": repo_name,
            "slices": len(contexts),
            "qa_total": len(repo_qas),
            "intrinsic": intrinsic_n,
            "extrinsic": extrinsic_n,
            "temporal": temporal_n,
            "output": {
                "all": str(qa_jsonl),
                "intrinsic": str(intrinsic_jsonl),
                "extrinsic": str(extrinsic_jsonl),
                "temporal": str(temporal_jsonl),
            },
        }
        with open(repo_out / "summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        overall_summary["repos"][repo_name] = summary
        overall_summary["total_qa"] += len(repo_qas)
        overall_summary["total_intrinsic"] += intrinsic_n
        overall_summary["total_extrinsic"] += extrinsic_n
        overall_summary["total_temporal"] += temporal_n

    with open(output_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(overall_summary, f, indent=2, ensure_ascii=False)

    return overall_summary
