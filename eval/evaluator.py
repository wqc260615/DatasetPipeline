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
from eval.llm_client import LLMClient
from eval.metrics import compute_metrics
from eval.prompt_builder import build_prompt

logger = logging.getLogger(__name__)


def load_qa_pairs(qa_file: Path) -> Iterator[dict]:
    with open(qa_file) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


_CODE_FENCE_RE = re.compile(r"```(?:\w+)?\n?(.*?)```", re.DOTALL)


def _strip_code_fence(text: str) -> str:
    """Remove markdown code fences, keeping only the inner content."""
    match = _CODE_FENCE_RE.search(text)
    if match:
        return match.group(1).strip()
    return text.strip()


def _context_retrieved(context: dict) -> bool:
    return (
        context.get("content") is not None
        or context.get("from_content") is not None
        or context.get("to_content") is not None
    )


def evaluate_batch(
    qa_pairs: list[dict],
    client: LLMClient,
    retriever: ContextRetriever,
    max_tokens: int = 256,
    batch_size: int = 8,
) -> list[dict]:
    """Evaluate a list of QA pairs in batches; return a result record for each."""
    # 1. Pre-compute contexts and prompts
    logger.info("Building prompts for %d QA pairs ...", len(qa_pairs))
    contexts = [retriever.get_context_for_qa(qa) for qa in qa_pairs]
    prompts = [build_prompt(qa, ctx) for qa, ctx in zip(qa_pairs, contexts)]

    # 2. Batch inference with progress bar
    n_batches = math.ceil(len(prompts) / batch_size)
    predictions = []
    for i in tqdm(range(n_batches), desc="Inferring"):
        batch = prompts[i * batch_size : (i + 1) * batch_size]
        predictions.extend(client.complete_batch(batch, max_tokens=max_tokens))

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
            "context_retrieved": _context_retrieved(ctx),
            "slice_id": qa.get("slice_id"),
            "from_slice_id": qa.get("from_slice_id"),
            "to_slice_id": qa.get("to_slice_id"),
            "metrics": {},
        }
        if prediction is not None:
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
