"""Evaluation metrics: Exact Match, token-level F1 (SQuAD-style), and ROUGE-L."""

from __future__ import annotations

import re
import string
from collections import Counter


def _normalize(s: str) -> str:
    """Lowercase, replace punctuation (excluding underscore) with spaces, strip articles."""
    s = s.lower()
    punct = string.punctuation.replace("_", "")
    s = s.translate(str.maketrans(punct, " " * len(punct)))
    s = re.sub(r"\b(a|an|the)\b", " ", s)
    return " ".join(s.split())


def exact_match(prediction: str, ground_truth: str) -> float:
    return float(_normalize(prediction) == _normalize(ground_truth))


def token_f1(prediction: str, ground_truth: str) -> float:
    pred_tokens = _normalize(prediction).split()
    truth_tokens = _normalize(ground_truth).split()

    if not pred_tokens or not truth_tokens:
        return float(pred_tokens == truth_tokens)

    common = Counter(pred_tokens) & Counter(truth_tokens)
    num_same = sum(common.values())
    if num_same == 0:
        return 0.0

    precision = num_same / len(pred_tokens)
    recall = num_same / len(truth_tokens)
    return (2 * precision * recall) / (precision + recall)


def _lcs_length(x: list[str], y: list[str]) -> int:
    """Space-efficient LCS length via rolling DP."""
    m, n = len(x), len(y)
    dp = [[0] * (n + 1) for _ in range(2)]
    for i in range(1, m + 1):
        row, prev = i % 2, (i - 1) % 2
        for j in range(1, n + 1):
            if x[i - 1] == y[j - 1]:
                dp[row][j] = dp[prev][j - 1] + 1
            else:
                dp[row][j] = max(dp[prev][j], dp[row][j - 1])
    return dp[m % 2][n]


def rouge_l(prediction: str, ground_truth: str) -> float:
    pred_tokens = _normalize(prediction).split()
    truth_tokens = _normalize(ground_truth).split()

    if not pred_tokens or not truth_tokens:
        return 0.0

    lcs = _lcs_length(pred_tokens, truth_tokens)
    if lcs == 0:
        return 0.0

    precision = lcs / len(pred_tokens)
    recall = lcs / len(truth_tokens)
    return (2 * precision * recall) / (precision + recall)


def yesno_em(prediction: str, ground_truth: str) -> float:
    """Extract the first word of the prediction and compare to Yes/No ground truth."""
    words = prediction.strip().split()
    if not words:
        return 0.0
    first = words[0].lower().rstrip(".,!?:;")
    return float(first == ground_truth.strip().lower())


def _normalize_version_str(s: str) -> str:
    """Strip leading 'v' from version numbers, e.g. 'v2.2.15' -> '2.2.15'."""
    return re.sub(r"\bv(\d)", r"\1", s)


def _normalize_temporal_change(s: str) -> str:
    """Normalise 'added: X; removed: Y' strings.

    Drops entries whose value is null/none/n/a, and lower-cases the key.
    E.g. 'added: release; removed: none' -> 'added: release'.
    """
    _NULL = {"null", "none", "n/a", ""}
    parts = [p.strip() for p in s.split(";") if p.strip()]
    kept = []
    for part in parts:
        m = re.match(r"^(added|removed)\s*:\s*(.+)$", part, re.IGNORECASE)
        if m:
            value = m.group(2).strip()
            if value.lower() not in _NULL:
                kept.append(f"{m.group(1).lower()}: {value}")
        else:
            kept.append(part)
    return "; ".join(kept)


def _normalize_item_list(s: str) -> str:
    """Normalise a comma/semicolon-separated list of identifiers.

    Strips backtick wrapping and 'this.' prefix from each item, deduplicates
    while preserving order, and rejoins with ', '.
    """
    parts = [p.strip() for p in re.split(r"[,;]", s) if p.strip()]
    seen: set[str] = set()
    result = []
    for p in parts:
        p = p.strip("`").strip()
        p = re.sub(r"^this\.", "", p)
        if p and p not in seen:
            seen.add(p)
            result.append(p)
    return ", ".join(sorted(result))


def _normalize_caller_list(s: str) -> str:
    """Normalise a comma/semicolon-separated list of caller method names.

    In addition to the item-list normalisations, strips class/object qualifiers
    such as 'new Foo().method' or 'Foo.method' -> 'method', and trailing '()'.
    """
    parts = [p.strip() for p in re.split(r"[,;]", s) if p.strip()]
    seen: set[str] = set()
    result = []
    for p in parts:
        p = p.strip("`").strip()
        p = re.sub(r"^new\s+\w+\([^)]*\)\.", "", p)
        p = re.sub(r"^\w+\.", "", p)
        p = re.sub(r"\(.*\)$", "", p).strip()
        if p and p not in seen:
            seen.add(p)
            result.append(p)
    return ", ".join(sorted(result))


_VERSION_SUBTYPES = {
    "function_first_introduced",
    "class_first_introduced",
    "function_last_present",
    "class_last_present",
    "function_introduced",
    "class_introduced",
    "function_not_introduced",
    "function_not_removed",
    "class_not_removed",
    "function_return_type_evolution",
}
_CHANGE_SUBTYPES = {
    "function_calls_changed",
    "function_instantiations_changed",
}


def _apply_subtype_normalization(
    pred: str, gt: str, qa_type: str, qa_subtype: str
) -> tuple[str, str]:
    """Apply subtype-specific pre-normalisation before metric computation."""
    if qa_type == "temporal":
        if qa_subtype in _VERSION_SUBTYPES:
            return _normalize_version_str(pred), _normalize_version_str(gt)
        if qa_subtype in _CHANGE_SUBTYPES:
            pred = _normalize_temporal_change(_normalize_version_str(pred))
            gt = _normalize_temporal_change(_normalize_version_str(gt))
            return pred, gt
    if qa_type == "intrinsic":
        if qa_subtype == "field_accesses":
            return _normalize_item_list(pred), _normalize_item_list(gt)
        if qa_subtype == "symbol_callers":
            return _normalize_caller_list(pred), _normalize_caller_list(gt)
    return pred, gt


def compute_metrics(
    prediction: str, ground_truth: str, qa_type: str, qa_subtype: str = ""
) -> dict:
    """Return the appropriate metrics dict for a given QA type/subtype."""
    prediction, ground_truth = _apply_subtype_normalization(
        prediction, ground_truth, qa_type, qa_subtype
    )
    if qa_type == "extrinsic":
        if qa_subtype == "yesno":
            return {"yesno_em": yesno_em(prediction, ground_truth)}
        return {"rouge_l": rouge_l(prediction, ground_truth)}
    else:  # intrinsic, temporal
        return {
            "em": exact_match(prediction, ground_truth),
            "f1": token_f1(prediction, ground_truth),
        }
