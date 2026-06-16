"""Core evaluation loop: run QA pairs through the LLM and compute metrics."""

from __future__ import annotations

import json
import logging
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterator

from tqdm import tqdm

from eval.context_retriever import ContextRetriever
from eval.llm_client import LLMClient, RemoteClient
from eval.metrics import compute_metrics
from eval.prompt_builder import (
    CONTEXT_UNAVAILABLE,
    build_prompt,
    build_question_only_prompt,
)

logger = logging.getLogger(__name__)


def load_qa_pairs(qa_file: Path) -> Iterator[dict]:
    with open(qa_file) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


# Match only properly-formed fences: optional language tag MUST be followed by a newline.
# This prevents greedily consuming answer words like "from" as a language identifier.
_CODE_FENCE_RE = re.compile(r"```\w*\n(.*?)```", re.DOTALL)
# Single-backtick wrapping: `answer`
_SINGLE_BACKTICK_RE = re.compile(r"^`([^`\n]+)`$")


def _strip_code_fence(text: str) -> str:
    """Remove markdown code fences or single-backtick wrapping, keeping inner content."""
    text = text.strip()
    match = _CODE_FENCE_RE.search(text)
    if match:
        content = match.group(1).strip()
        if content:
            return content
    # Handle single-backtick-wrapped answer, e.g. `No` or `0.75.2`
    m = _SINGLE_BACKTICK_RE.match(text)
    if m:
        return m.group(1).strip()
    return text


def _context_retrieved(context: dict) -> bool:
    return (
        context.get("content") is not None
        or context.get("from_content") is not None
        or context.get("to_content") is not None
    )


def evaluate_batch(
    qa_pairs: list[dict],
    client: LLMClient | RemoteClient,
    retriever: ContextRetriever | None,
    max_tokens: int = 256,
    batch_size: int = 1,
    retry_null_max_tokens: int | None = None,
    question_only: bool = False,
) -> list[dict]:
    """Evaluate a list of QA pairs in batches; return a result record for each."""
    # 1. Pre-compute contexts and prompts
    prompt_mode = "question_only" if question_only else "slice_context"
    if question_only:
        logger.info(
            "Building question-only prompts for %d QA pairs ...", len(qa_pairs)
        )
        contexts = [{} for _ in qa_pairs]
        prompts = [build_question_only_prompt(qa) for qa in qa_pairs]
    else:
        if retriever is None:
            raise ValueError("retriever is required unless question_only=True")
        logger.info("Building prompts for %d QA pairs ...", len(qa_pairs))
        contexts = [retriever.get_context_for_qa(qa) for qa in qa_pairs]
        prompts = [build_prompt(qa, ctx) for qa, ctx in zip(qa_pairs, contexts)]

    # 2. Batch inference — skip items with no context (prompt is None)
    answerable_indices = [i for i, p in enumerate(prompts) if p is not None]
    answerable_prompts = [prompts[i] for i in answerable_indices]
    n_skipped = len(prompts) - len(answerable_prompts)
    if n_skipped:
        logger.warning("Skipping %d QA pairs with no retrievable context.", n_skipped)

    n_batches = math.ceil(len(answerable_prompts) / batch_size) if answerable_prompts else 0
    answerable_predictions: list = []
    for i in tqdm(range(n_batches), desc="Inferring"):
        batch = answerable_prompts[i * batch_size : (i + 1) * batch_size]
        answerable_predictions.extend(client.complete_batch(batch, max_tokens=max_tokens))

    # Reconstruct full prediction list; unanswerable slots get the sentinel
    predictions: list = [CONTEXT_UNAVAILABLE] * len(prompts)
    for idx, pred in zip(answerable_indices, answerable_predictions):
        predictions[idx] = pred

    # One recovery pass for context-backed items where the provider returned no
    # usable content after its own retries. Context misses stay as the sentinel.
    if retry_null_max_tokens:
        null_retry_indices = [
            idx for idx in answerable_indices if predictions[idx] is None
        ]
        if null_retry_indices:
            logger.warning(
                "Retrying %d QA pairs with no prediction using max_tokens=%d.",
                len(null_retry_indices),
                retry_null_max_tokens,
            )
            null_retry_prompts = [prompts[i] for i in null_retry_indices]
            null_retry_predictions: list = []
            n_retry_batches = math.ceil(len(null_retry_prompts) / batch_size)
            for i in tqdm(range(n_retry_batches), desc="Retrying nulls"):
                batch = null_retry_prompts[i * batch_size : (i + 1) * batch_size]
                null_retry_predictions.extend(
                    client.complete_batch(batch, max_tokens=retry_null_max_tokens)
                )
            recovered = 0
            for idx, pred in zip(null_retry_indices, null_retry_predictions):
                if pred is not None:
                    predictions[idx] = pred
                    recovered += 1
            logger.info(
                "Recovered %d/%d missing predictions.",
                recovered,
                len(null_retry_indices),
            )

    # 3. Assemble result records
    results: list[dict] = []
    for qa, ctx, prediction in zip(qa_pairs, contexts, predictions):
        result: dict = {
            "qa_id": qa["qa_id"],
            "repo": qa["repo"],
            "qa_type": qa["qa_type"],
            "qa_subtype": qa["qa_subtype"],
            "question": qa["question"],
            "ground_truth": qa["answer"],
            "prediction": prediction,
            "prompt_mode": prompt_mode,
            "context_retrieved": _context_retrieved(ctx),
            "slice_id": qa.get("slice_id"),
            "from_slice_id": qa.get("from_slice_id"),
            "to_slice_id": qa.get("to_slice_id"),
            "metrics": {},
        }
        if prediction == CONTEXT_UNAVAILABLE:
            # No source code available; skip metrics, keep sentinel as prediction
            logger.warning("No context for qa_id=%s — prediction set to %r", qa["qa_id"], CONTEXT_UNAVAILABLE)
        elif prediction is not None:
            pred = _strip_code_fence(prediction)
            result["prediction"] = pred
            result["metrics"] = compute_metrics(pred, qa["answer"], qa["qa_type"], qa.get("qa_subtype", ""))
        else:
            logger.warning("No prediction for qa_id=%s", qa["qa_id"])
        results.append(result)
    return results


def aggregate_metrics(results: list[dict]) -> dict:
    """Compute mean metrics grouped by qa_type/qa_subtype."""
    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        if r["metrics"]:
            key = f"{r['qa_type']}/{r['qa_subtype']}"
            buckets[key].append(r["metrics"])

    agg: dict[str, dict] = {}
    for key, metric_list in sorted(buckets.items()):
        all_keys = {k for m in metric_list for k in m}
        agg[key] = {
            k: sum(m[k] for m in metric_list if k in m) / len(metric_list)
            for k in all_keys
        }
    return agg
