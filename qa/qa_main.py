"""CLI entrypoint for QA generation from precomputed slices."""

from __future__ import annotations

import argparse
from pathlib import Path

from qa.qa_generator import generate_qa_dataset


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate QA pairs from data/slices symbols output"
    )
    parser.add_argument(
        "--slices-root",
        type=str,
        default="data/slices",
        help="Root directory that contains per-repository slices",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/qa",
        help="Output directory for generated QA data",
    )
    parser.add_argument(
        "--repos",
        type=str,
        required=True,
        help="Comma-separated repository names, required (e.g. colorama,fastapi)",
    )
    args = parser.parse_args()

    repos = [r.strip() for r in args.repos.split(",") if r.strip()] or None

    summary = generate_qa_dataset(
        slices_root=Path(args.slices_root),
        output_dir=Path(args.output_dir),
        repos=repos,
    )

    print("QA generation completed")
    print(f"Total QA: {summary['total_qa']}")
    print(f"Intrinsic: {summary['total_intrinsic']}")
    print(f"Extrinsic: {summary['total_extrinsic']}")
    print(f"Temporal: {summary['total_temporal']}")


if __name__ == "__main__":
    main()
