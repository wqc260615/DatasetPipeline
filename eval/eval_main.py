"""CLI entry point for LLM evaluation on semantic evolution slice QA pairs."""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
from datetime import datetime
from pathlib import Path

from eval.context_retriever import ContextRetriever
from eval.evaluator import aggregate_metrics, evaluate_batch, load_qa_pairs
from eval.llm_client import LLMClient, RemoteClient

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


def _safe_model_dir_name(model_name: str) -> str:
    """Convert model IDs like org/model into a single filesystem-safe name."""
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "__", model_name.strip())
    return safe_name.strip("._-") or "unknown_model"


def _format_summary(
    *,
    repo: str,
    tag: str,
    model_name: str,
    prompt_mode: str,
    results: list[dict],
    agg: dict[str, dict[str, float]],
) -> str:
    no_prediction = sum(1 for r in results if r["prediction"] is None)
    context_miss = sum(1 for r in results if not r["context_retrieved"])
    context_label = (
        "Context absent" if prompt_mode == "question_only" else "Context missing"
    )

    lines = [
        f"=== Evaluation summary ({repo} / {tag}) ===",
        f"  Model           : {model_name}",
        f"  Prompt mode     : {prompt_mode}",
        f"  Total evaluated : {len(results)}",
        f"  No prediction   : {no_prediction}",
        f"  {context_label:<15}: {context_miss}",
        "",
        "  Metrics by subtype:",
    ]
    for key, metrics in agg.items():
        parts = ", ".join(f"{k}={v:.4f}" for k, v in metrics.items())
        lines.append(f"    {key}: {parts}")
    return "\n".join(lines)


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
        default=1,
        help="Number of prompts per inference batch (default: 1)",
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
    parser.add_argument(
        "--question-only",
        action="store_true",
        help=(
            "Run a no-context control: send only each QA question to the model, "
            "without retrieving slices or source/reference files."
        ),
    )

    parser.add_argument(
        "--api-key",
        default=None,
        metavar="API_KEY",
        help="API key for a remote OpenAI-compatible endpoint. When provided, use remote inference instead of local.",
    )
    parser.add_argument(
        "--api-model",
        default=RemoteClient.DEFAULT_MODEL,
        help=f"Remote model ID (default: {RemoteClient.DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--api-base-url",
        default=RemoteClient.DEFAULT_BASE_URL,
        help=f"Remote API base URL (default: {RemoteClient.DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--api-workers",
        type=int,
        default=1,
        help="Max concurrent API requests when using remote inference (default: 1)",
    )
    parser.add_argument(
        "--api-temperature",
        type=float,
        default=0.0,
        help="Sampling temperature for remote inference (default: 0.0 for eval reproducibility)",
    )
    parser.add_argument(
        "--api-reasoning-effort",
        choices=["none", "low", "medium", "high", "max", "xhigh"],
        default=None,
        help=(
            "Reasoning effort for remote reasoning models. Use 'none' for "
            "providers that support it; for DeepSeek, 'none' maps to "
            "thinking=disabled."
        ),
    )
    parser.add_argument(
        "--api-thinking",
        choices=["enabled", "disabled"],
        default=None,
        help="Provider-specific thinking-mode toggle, currently useful for DeepSeek V4.",
    )
    parser.add_argument(
        "--retry-null-max-tokens",
        type=int,
        default=None,
        help=(
            "Max tokens for one extra pass over context-backed QA pairs whose "
            "prediction is still null. For remote inference, defaults to up to "
            "4x --max-tokens capped at 4096; set 0 to disable."
        ),
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

    retriever = None
    if args.question_only:
        logger.info(
            "Question-only control enabled: skipping slice/context retrieval."
        )
    else:
        retriever = ContextRetriever(
            slices_root=Path(args.slices_dir),
            repos_root=Path(args.repos_dir),
        )
    active_model = args.api_model if args.api_key else args.model
    if args.api_key:
        client: LLMClient | RemoteClient = RemoteClient(
            api_key=args.api_key,
            model=active_model,
            base_url=args.api_base_url,
            max_workers=args.api_workers,
            temperature=args.api_temperature,
            reasoning_effort=args.api_reasoning_effort,
            thinking=args.api_thinking,
        )
    else:
        client = LLMClient(
            model=active_model,
            device_map=args.device_map,
            batch_size=args.batch_size,
        )

    retry_null_max_tokens = args.retry_null_max_tokens
    if retry_null_max_tokens is None and args.api_key:
        retry_null_max_tokens = max(
            args.max_tokens,
            min(args.max_tokens * 4, RemoteClient.EMPTY_LENGTH_TOKEN_CAP),
        )
    if retry_null_max_tokens is not None and retry_null_max_tokens <= 0:
        retry_null_max_tokens = None

    results = evaluate_batch(
        qa_pairs, client, retriever,
        max_tokens=args.max_tokens,
        batch_size=args.batch_size,
        retry_null_max_tokens=retry_null_max_tokens,
        question_only=args.question_only,
    )

    model_output_dir = Path(args.output_dir) / _safe_model_dir_name(active_model)
    model_output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = args.subtype or (Path(args.qa_file).stem if args.qa_file else args.qa_type)
    prompt_mode = "question_only" if args.question_only else "slice_context"
    if args.question_only:
        tag = f"{tag}_question_only"
    base_name = f"{args.repo}_{tag}_{timestamp}"
    out_file = model_output_dir / f"{base_name}.jsonl"
    with open(out_file, "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    logger.info("Results written to %s", out_file)

    agg = aggregate_metrics(results)
    summary = _format_summary(
        repo=args.repo,
        tag=tag,
        model_name=active_model,
        prompt_mode=prompt_mode,
        results=results,
        agg=agg,
    )
    summary_file = model_output_dir / f"{base_name}_summary.txt"
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(summary + "\n")
    logger.info("Summary written to %s", summary_file)

    print(f"\n{summary}")


if __name__ == "__main__":
    main()
