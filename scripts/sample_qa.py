"""
Stratified QA sampler — repo-agnostic, config-driven.

Usage:
    python scripts/sample_qa.py --repo fastapi --budget 5000 --output data/qa/fastapi/sampled.jsonl
    python scripts/sample_qa.py --repo spring-boot --budget 5000 --output data/qa/spring-boot/sampled.jsonl
    python scripts/sample_qa.py --repo fastapi spring-boot --budget 5000 --output data/qa/sampled_all.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import random
from collections import defaultdict
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sampling configuration (repo-agnostic)
# ---------------------------------------------------------------------------

# 1. Type-level budget allocation (must sum to 1.0).
#    For repos with very few extrinsic QAs, the extrinsic slot is filled as
#    much as possible and the remainder is redistributed to intrinsic.
TYPE_ALLOC = {
    "intrinsic": 0.48,
    "extrinsic": 0.10,
    "temporal":  0.42,
}

# 2. Subtype-level weights WITHIN each type.
#    Higher weight means more samples drawn from that subtype.
#    Subtypes absent in a repo are simply ignored.
SUBTYPE_WEIGHTS: dict[str, dict[str, float]] = {
    "intrinsic": {
        # High value: require precise code-text answers
        "function_signature":           5.0,
        "function_return_type":         4.0,
        "function_parameters":          2.0,   # overlaps with signature; lower weight
        "symbol_callers":               3.0,
        "class_inheritance":            3.0,
        "class_methods":                2.5,
        "class_fields":                 2.5,
        "field_accesses":               2.0,
        "object_instantiations":        2.0,
        "class_instantiation_sites":    2.0,
        "class_implemented_interfaces": 2.0,
        "interface_implementors":       2.0,
        "class_subclasses":             1.5,
        # Low value: trivial Yes/No or trivially-location answers → downweight heavily
        "symbol_existence":             0.5,
        "symbol_location":              0.2,   # almost always "Yes", near-zero signal
        "class_existence":              0.3,
        "symbol_call_sites":            1.0,
    },
    "extrinsic": {
        # Take everything available (extrinsic QAs are scarce and high-value)
        "__all__": 1.0,
    },
    "temporal": {
        # High value: answer is a descriptive text, not just Yes/No
        "function_signature_evolution":  5.0,
        "function_signature_changed":    5.0,
        "function_return_type_evolution":4.0,
        "function_return_type_changed":  4.0,
        "function_calls_changed":        4.0,
        "class_inheritance_evolution":   4.0,
        "class_inheritance_changed":     4.0,
        "function_instantiations_changed":3.0,
        # Medium value: Yes/No but semantically meaningful
        "function_introduced":           1.5,
        "function_removed":              1.5,
        "class_introduced":              1.5,
        "class_removed":                 1.5,
        # Ordering questions (Scenario 1 in proposal): first introduced / last present
        # Answer is a version string, not Yes/No — lower weight since pools are
        # large and these questions are lower priority than changed/evolution.
        "function_first_introduced":     0.3,
        "function_last_present":         0.3,
        "class_first_introduced":        0.3,
        "class_last_present":            0.3,
        # Negative / stable subtypes: critical for RQ3 Stability metric.
        # Pool is small (cap 380/190) so weight is set high to claim full pool
        # before large intro/removed pools crowd them out.
        "function_not_introduced":       5.0,
        "function_not_removed":          5.0,
        "class_not_introduced":          5.0,
        "class_not_removed":             5.0,
        # Unchanged subtypes: RQ3 Stability ground-truth (pool capped at 190 each)
        "function_signature_unchanged":  10.0,
        "function_return_type_unchanged":10.0,
        "class_inheritance_unchanged":   10.0,
    },
}

import re as _re

_TEST_PATH_RE = _re.compile(
    r"(?:^|[/\\])tests?[/\\]"
    r"|(?:^|[/\\])test_"
    r"|_tests?\.py$"
    r"|[/\\]src[/\\]test[/\\]"
    r"|Test\.java$"
    r"|Tests\.java$",
    _re.IGNORECASE,
)

# 3. Quality filters applied BEFORE sampling.
FILTERS = {
    # For binary (Yes/No) subtypes: cap the Yes-ratio to avoid skew.
    # Any subtype whose answers are >80% Yes will be balanced to 50/50.
    "binary_subtypes": {
        "symbol_existence", "symbol_location", "class_existence",
        "function_introduced", "function_removed",
        "class_introduced", "class_removed",
        "function_not_introduced", "function_not_removed",
        "class_not_introduced", "class_not_removed",
        "function_signature_unchanged", "function_return_type_unchanged",
        "class_inheritance_unchanged",
    },
    # Maximum QA pairs contributed by any single evidence file.
    # Prevents tutorial-style repos (fastapi/docs/tutorial/*) from dominating.
    "max_per_file": 8,
    # Subtypes where a trivial answer ("none", "()", etc.) carries no evaluation
    # signal: the answer merely confirms absence.  Keeping these inflates easy
    # questions and lets an LLM score high by always guessing "none".
    # NOTE: function_return_type is intentionally excluded — "None" is a real
    # Python return type and is a meaningful, non-trivial answer.
    # NOTE: class_inheritance is intentionally excluded — "none" (no base
    # classes) is a meaningful negative fact worth testing.
    "trivial_answer_subtypes": {
        "class_subclasses",
        "class_implemented_interfaces",
        "class_methods",
        "class_fields",
        "function_parameters",
        "interface_implementors",
    },
    "trivial_answers": {"none", "()", "[]", "{}"},
    # Filter questions whose evidence comes from test files.  The temporal
    # generator already applies this via _prod_function/_prod_class; here we
    # catch any test-path items that slip through in intrinsic generation.
    "filter_test_paths": True,
}

# ---------------------------------------------------------------------------
# Core sampling logic
# ---------------------------------------------------------------------------

def load_jsonl(path: Path):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _balance_binary(items: list[dict]) -> list[dict]:
    """For a binary-answer subtype, return equal Yes/No items (min of both)."""
    yes = [x for x in items if x["answer"].strip().lower() == "yes"]
    no  = [x for x in items if x["answer"].strip().lower() == "no"]
    n = min(len(yes), len(no))
    if n == 0:
        return items  # can't balance, return as-is
    return yes[:n] + no[:n]


def _cap_per_file(items: list[dict], max_per_file: int) -> list[dict]:
    """Keep at most max_per_file items per evidence file_path."""
    counts: dict[str, int] = defaultdict(int)
    result = []
    for item in items:
        fp = item.get("evidence", {}).get("file_path", "__unknown__")
        if counts[fp] < max_per_file:
            counts[fp] += 1
            result.append(item)
    return result


def _collect_pool(qa_dir: Path, qa_type: str) -> dict[str, list[dict]]:
    """Load all QA pairs for a type and group by subtype."""
    f = qa_dir / f"{qa_type}_qa_pairs.jsonl"
    if not f.exists():
        return {}
    pool: dict[str, list[dict]] = defaultdict(list)
    for item in load_jsonl(f):
        pool[item["qa_subtype"]].append(item)
    return pool


def _sample_type(
    pool: dict[str, list[dict]],
    qa_type: str,
    budget: int,
    seed: int,
) -> list[dict]:
    """Weighted stratified sampling within a single QA type."""
    if not pool:
        return []

    rng = random.Random(seed)
    weights = SUBTYPE_WEIGHTS.get(qa_type, {})
    binary_subtypes = FILTERS["binary_subtypes"]
    max_per_file = FILTERS["max_per_file"]
    trivial_answer_subtypes = FILTERS["trivial_answer_subtypes"]
    trivial_answers = FILTERS["trivial_answers"]
    filter_test_paths = FILTERS["filter_test_paths"]

    # Build filtered sub-pools
    filtered: dict[str, list[dict]] = {}
    for subtype, items in pool.items():
        w = weights.get(subtype, weights.get("__all__", 1.0))
        if w == 0.0:
            logger.debug("  Skipping subtype=%s (weight=0)", subtype)
            continue

        # Remove test-path items (intrinsic generator does not filter these)
        if filter_test_paths:
            items = [x for x in items
                     if not _TEST_PATH_RE.search(x.get("evidence", {}).get("file_path") or "")]

        # Remove trivial-answer items for subtypes where absence == "none"/"()"
        if subtype in trivial_answer_subtypes:
            items = [x for x in items
                     if x.get("answer", "").strip().lower() not in trivial_answers]

        # Balance binary subtypes
        if subtype in binary_subtypes:
            items = _balance_binary(items)

        # Shuffle before file-cap so the cap isn't biased toward file-sorted order
        rng.shuffle(items)
        items = _cap_per_file(items, max_per_file)
        if items:
            filtered[subtype] = items

    if not filtered:
        return []

    # Compute how many to draw from each subtype (proportional to weight).
    # Pool size is capped at POOL_CAP for the weight calculation only — the
    # actual draw can still use the full filtered pool.  This prevents subtypes
    # with very large pools (e.g. spring-boot function_introduced ~40k) from
    # crowding out small but evaluation-critical subtypes (e.g. *_unchanged
    # pools capped at 190) even when those small subtypes carry high weights.
    POOL_CAP = 2000
    total_weight = sum(
        weights.get(st, weights.get("__all__", 1.0)) * min(len(items), POOL_CAP)
        for st, items in filtered.items()
    )
    allocations: dict[str, int] = {}
    allocated = 0
    subtypes_sorted = sorted(filtered.keys())  # deterministic order
    for st in subtypes_sorted:
        w = weights.get(st, weights.get("__all__", 1.0))
        n = round(budget * w * min(len(filtered[st]), POOL_CAP) / total_weight)
        n = min(n, len(filtered[st]))
        allocations[st] = n
        allocated += n

    # Fix rounding drift: add/remove from the highest-weight subtype
    drift = allocated - budget
    if drift != 0:
        anchor = max(subtypes_sorted, key=lambda s: weights.get(s, 1.0))
        allocations[anchor] = max(0, allocations[anchor] - drift)

    result = []
    for st in subtypes_sorted:
        n = allocations.get(st, 0)
        if n > 0:
            sampled = rng.sample(filtered[st], min(n, len(filtered[st])))
            result.extend(sampled)
            logger.info("    %-40s  drawn=%4d / pool=%6d", st, len(sampled), len(filtered[st]))

    return result


def _estimate_drawable(pool: dict[str, list[dict]], qa_type: str, seed: int) -> int:
    """Estimate how many items are actually drawable after all filters."""
    if not pool:
        return 0
    rng = random.Random(seed)
    weights = SUBTYPE_WEIGHTS.get(qa_type, {})
    binary_subtypes = FILTERS["binary_subtypes"]
    max_per_file = FILTERS["max_per_file"]
    trivial_answer_subtypes = FILTERS["trivial_answer_subtypes"]
    trivial_answers = FILTERS["trivial_answers"]
    total = 0
    for subtype, items in pool.items():
        w = weights.get(subtype, weights.get("__all__", 1.0))
        if w == 0.0:
            continue
        if FILTERS["filter_test_paths"]:
            items = [x for x in items
                     if not _TEST_PATH_RE.search(x.get("evidence", {}).get("file_path") or "")]
        if subtype in trivial_answer_subtypes:
            items = [x for x in items
                     if x.get("answer", "").strip().lower() not in trivial_answers]
        if subtype in binary_subtypes:
            items = _balance_binary(items)
        shuffled = list(items)
        rng.shuffle(shuffled)
        items = _cap_per_file(shuffled, max_per_file)
        total += len(items)
    return total


def sample_repo(
    qa_dir: Path,
    budget: int,
    seed: int = 42,
) -> list[dict]:
    """Full stratified sample for one repository."""
    # Pre-load all pools (needed for drawable estimation)
    pools = {qt: _collect_pool(qa_dir, qt) for qt in TYPE_ALLOC}

    # Estimate drawable items per type after filtering
    drawable = {qt: _estimate_drawable(pools[qt], qt, seed) for qt in TYPE_ALLOC}
    logger.info("Drawable pool sizes: %s", drawable)

    # Allocate budget: honour TYPE_ALLOC ratios but cap at drawable,
    # then redistribute any surplus to types that still have capacity.
    raw_budgets = {qt: int(budget * frac) for qt, frac in TYPE_ALLOC.items()}
    # Fix integer rounding: assign remainder to intrinsic
    raw_budgets["intrinsic"] += budget - sum(raw_budgets.values())

    budgets: dict[str, int] = {}
    surplus = 0
    for qt in ["extrinsic", "temporal", "intrinsic"]:  # extrinsic first (scarcest)
        allocated = min(raw_budgets[qt] + surplus, drawable[qt])
        surplus = max(0, raw_budgets[qt] + surplus - drawable[qt])
        budgets[qt] = allocated
    # Any remaining surplus goes to intrinsic (largest pool)
    budgets["intrinsic"] = min(budgets["intrinsic"] + surplus, drawable["intrinsic"])

    logger.info("Budget allocation: %s", budgets)

    results = []
    for qa_type, type_budget in budgets.items():
        logger.info("Sampling type=%s  budget=%d", qa_type, type_budget)
        sampled = _sample_type(pools[qa_type], qa_type, type_budget, seed)
        logger.info("  → %d sampled for %s", len(sampled), qa_type)
        results.extend(sampled)

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Stratified QA sampler")
    parser.add_argument(
        "--repo", nargs="+", required=True,
        help="One or more repo names (e.g. fastapi spring-boot)"
    )
    parser.add_argument(
        "--budget", type=int, default=5000,
        help="Target number of QA pairs PER REPO (default: 5000)"
    )
    parser.add_argument(
        "--qa-dir", default="data/qa",
        help="Root QA directory (default: data/qa)"
    )
    parser.add_argument(
        "--output", default=None,
        help="Output JSONL path. If multiple repos, items from all repos are merged. "
             "Default: data/qa/{repo}/sampled.jsonl (per-repo) or data/qa/sampled_all.jsonl"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    qa_root = Path(args.qa_dir)
    all_results = []

    for repo in args.repo:
        qa_dir = qa_root / repo
        if not qa_dir.exists():
            logger.error("QA dir not found: %s", qa_dir)
            continue
        logger.info("==== Sampling repo: %s ====", repo)
        sampled = sample_repo(qa_dir, budget=args.budget, seed=args.seed)
        logger.info("Total sampled for %s: %d", repo, len(sampled))

        # Per-repo output when processing multiple repos
        if len(args.repo) > 1 or args.output is None:
            per_repo_out = qa_dir / "sampled.jsonl"
            with open(per_repo_out, "w") as f:
                for item in sampled:
                    f.write(json.dumps(item) + "\n")
            logger.info("Written to %s", per_repo_out)

        all_results.extend(sampled)

    # Combined output
    if args.output:
        out_path = Path(args.output)
    elif len(args.repo) > 1:
        out_path = qa_root / "sampled_all.jsonl"
    else:
        out_path = qa_root / args.repo[0] / "sampled.jsonl"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for item in all_results:
            f.write(json.dumps(item) + "\n")
    logger.info("All repos combined → %s  (%d items)", out_path, len(all_results))

    # Print summary table
    from collections import Counter
    print("\n=== Sampling summary ===")
    by_repo_type = Counter((r["repo"], r["qa_type"], r["qa_subtype"]) for r in all_results)
    current = None
    for (repo, qtype, subtype), n in sorted(by_repo_type.items()):
        if (repo, qtype) != current:
            print(f"\n  {repo} / {qtype}")
            current = (repo, qtype)
        print(f"    {subtype:<45} {n:>5}")
    print(f"\n  TOTAL: {len(all_results)}")


if __name__ == "__main__":
    main()
