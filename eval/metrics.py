"""Evaluation metrics: Exact Match, token-level F1 (SQuAD-style), and ROUGE-L."""

from __future__ import annotations

import re
import string
from collections import Counter


# ---------------------------------------------------------------------------
# Answer normalisation (SQuAD convention)
# ---------------------------------------------------------------------------

def _normalize(s: str) -> str:
    """Lowercase, replace punctuation (excluding underscore) with spaces, strip articles."""
    s = s.lower()
    punct = string.punctuation.replace("_", "")
    s = s.translate(str.maketrans(punct, " " * len(punct)))
    s = re.sub(r"\b(a|an|the)\b", " ", s)
    return " ".join(s.split())


# ---------------------------------------------------------------------------
# Exact Match
# ---------------------------------------------------------------------------

def exact_match(prediction: str, ground_truth: str) -> float:
    return float(_normalize(prediction) == _normalize(ground_truth))


# ---------------------------------------------------------------------------
# Token-level F1  (SQuAD)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# ROUGE-L  (F-measure via token LCS)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Yes/No  (first-word EM)
# ---------------------------------------------------------------------------

def yesno_em(prediction: str, ground_truth: str) -> float:
    """Extract the first word of the prediction and compare to Yes/No ground truth."""
    words = prediction.strip().split()
    if not words:
        return 0.0
    first = words[0].lower().rstrip(".,!?:;")
    return float(first == ground_truth.strip().lower())


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def compute_metrics(
    prediction: str, ground_truth: str, qa_type: str, qa_subtype: str = ""
) -> dict:
    """Return the appropriate metrics dict for a given QA type/subtype."""
    if qa_type == "extrinsic":
        if qa_subtype == "yesno":
            return {"yesno_em": yesno_em(prediction, ground_truth)}
        return {"rouge_l": rouge_l(prediction, ground_truth)}
    else:  # intrinsic, temporal
        return {
            "em": exact_match(prediction, ground_truth),
            "f1": token_f1(prediction, ground_truth),
        }
