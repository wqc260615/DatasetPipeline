"""CLI entry point for LLM evaluation on semantic evolution slice QA pairs."""

from __future__ import annotations

import argparse
import json
import logging
import random
from datetime import datetime
from pathlib import Path

from eval.context_retriever import ContextRetriever
from eval.evaluator import aggregate_metrics, evaluate_batch, load_qa_pairs
from eval.llm_client import LLMClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

QA_TYPE_CHOICES = ["intrinsic", "extrinsic", "temporal", "all"]


def _qa_file(qa_dir: Path, qa_type: str) -> Path:
    if qa_type == "all":
        return qa_dir / "qa_pairs.jsonl"
    return qa_dir / f"{qa_type}_qa_pairs.jsonl"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate an LLM on slice-grounded QA pairs"
    )
    parser.add_argument("--repo", required=True, help="Repository name (e.g. fastapi)")
    parser.add_argument(
        "--qa-type",
        default="intrinsic",
        choices=QA_TYPE_CHOICES,
        help="QA type to evaluate (default: intrinsic)",
    )
    parser.add_argument(
        "--subtype",
        default=None,
        help="Filter to a specific qa_subtype (e.g. function_signature)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Randomly sample N QA pairs before evaluation",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for sampling")
    parser.add_argument(
        "--model",
        default="Qwen/Qwen2.5-Coder-7B-Instruct",
        help="HuggingFace model ID",
    )
    parser.add_argument(
        "--device-map",
        default="auto",
        help="Device map passed to the HuggingFace pipeline (default: auto)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=8,
        help="Number of prompts per inference batch (default: 8)",
    )
    parser.add_argument(
        "--max-tokens", type=int, default=256, help="Max tokens for each LLM response"
    )
    parser.add_argument(
        "--qa-file",
        default=None,
        help="Direct path to a QA JSONL file (overrides --qa-dir / --qa-type)",
    )
    parser.add_argument(
        "--qa-dir", default="data/qa", help="Root directory for QA pairs"
    )
    parser.add_argument(
        "--slices-dir", default="data/slices", help="Root directory for slices"
    )
    parser.add_argument(
        "--repos-dir",
        default="data/repositories",
        help="Root directory for cloned repositories",
    )
    parser.add_argument(
        "--output-dir",
        default="data/eval_results",
        help="Directory to write result JSONL files",
    )
    args = parser.parse_args()

    qa_file = (
        Path(args.qa_file)
        if args.qa_file
        else _qa_file(Path(args.qa_dir) / args.repo, args.qa_type)
    )
    if not qa_file.exists():
        logger.error("QA file not found: %s", qa_file)
        return

    logger.info("Loading QA pairs from %s", qa_file)
    qa_pairs = list(load_qa_pairs(qa_file))
    logger.info("Loaded %d QA pairs", len(qa_pairs))

    if args.subtype:
        qa_pairs = [q for q in qa_pairs if q["qa_subtype"] == args.subtype]
        logger.info("Filtered to subtype '%s': %d pairs", args.subtype, len(qa_pairs))

    if args.sample and args.sample < len(qa_pairs):
        random.seed(args.seed)
        qa_pairs = random.sample(qa_pairs, args.sample)
        logger.info("Sampled %d pairs (seed=%d)", len(qa_pairs), args.seed)

    retriever = ContextRetriever(
        slices_root=Path(args.slices_dir),
        repos_root=Path(args.repos_dir),
    )
    client = LLMClient(
        model=args.model,
        device_map=args.device_map,
        batch_size=args.batch_size,
    )

    results = evaluate_batch(
        qa_pairs, client, retriever,
        max_tokens=args.max_tokens,
        batch_size=args.batch_size,
    )

    # Write per-item results
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = args.subtype or (Path(args.qa_file).stem if args.qa_file else args.qa_type)
    out_file = output_dir / f"{args.repo}_{tag}_{timestamp}.jsonl"
    with open(out_file, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    logger.info("Results written to %s", out_file)

    # Print aggregate metrics
    agg = aggregate_metrics(results)
    no_prediction = sum(1 for r in results if r["prediction"] is None)
    context_miss = sum(1 for r in results if not r["context_retrieved"])

    print(f"\n=== Evaluation summary ({args.repo} / {tag}) ===")
    print(f"  Total evaluated : {len(results)}")
    print(f"  No prediction   : {no_prediction}")
    print(f"  Context missing : {context_miss}")
    print("\n  Metrics by subtype:")
    for key, metrics in agg.items():
        parts = ", ".join(f"{k}={v:.4f}" for k, v in metrics.items())
        print(f"    {key}: {parts}")


if __name__ == "__main__":
    main()
