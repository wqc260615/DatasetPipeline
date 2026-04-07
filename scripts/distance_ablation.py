#!/usr/bin/env python3
"""
Weight-Sensitivity Ablation Study for Semantic Distance.

Runs the dynamic-programming selector over a range of heuristic weight combinations
({api: 0}, {api: 0.1}, {api: 0.3}, {api: 0.5}) with symmetric lines & files weights.
Outputs the Jaccard similarity (relative to baseline api=0.1) and coefficient of
variation (CV) of the chosen slice distance distribution.
"""

import argparse
import logging
import statistics
import sys
from pathlib import Path

# Ensure pipeline is importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from git import Repo

# Imports from pipeline
from pipeline.config import load_config, Config
from pipeline.semantic_slicer import (
    collect_tag_anchors,
    compute_adjacent_tag_metrics,
    normalize_tag_pair_metrics,
    select_tag_slices_dp,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("ablation")


def mean_index_shift(baseline_indices: list, test_indices: list) -> float:
    """
    Compute the Mean Absolute Error (MAE) between the selected anchor indices.
    This effectively measures how many 'versions' a slice boundary shifted on average.
    """
    if not baseline_indices or not test_indices or len(baseline_indices) != len(test_indices):
        return 0.0
    shifts = [abs(b - t) for b, t in zip(baseline_indices, test_indices)]
    return sum(shifts) / len(shifts)


def compute_segment_distance_cv(
    normalised_metrics: list,
    selected_indices: list
) -> float:
    """
    Compute the Coefficient of Variation (CV = std/mean) for the 
    distances of the selected segments.
    """
    if len(selected_indices) < 2:
        return 0.0

    # Build prefix sum of distances
    dist_by_idx = {i: nm.distance for i, nm in enumerate(normalised_metrics)}
    
    segment_distances = []
    for pos in range(1, len(selected_indices)):
        idx = selected_indices[pos]
        prev_idx = selected_indices[pos - 1]
        
        # Accumulate distance for this segment
        seg_dist = sum(dist_by_idx.get(k, 0.0) for k in range(prev_idx, idx))
        segment_distances.append(seg_dist)
        
    if not segment_distances:
        return 0.0
        
    mean_dist = statistics.mean(segment_distances)
    if mean_dist == 0:
        return 0.0
        
    if len(segment_distances) == 1:
        return 0.0  # std is 0 if there's only one segment

    std_dist = statistics.stdev(segment_distances)
    cv = std_dist / mean_dist
    return cv


def run_ablation(repo_path: Path, config: Config):
    repo = Repo(repo_path)
    
    # 1. Collect tag anchors and compute raw pair metrics once
    logger.info("Collecting tag anchors...")
    anchors = collect_tag_anchors(repo, config.slicing)
    
    logger.info("Computing raw adjacent tag metrics (API break detection & diffs)...")
    pair_metrics = compute_adjacent_tag_metrics(repo, anchors, config.slicing)
    
    if not anchors or not pair_metrics:
        logger.error("Insufficient anchors or pair metrics found.")
        return

    # 2. Define weight configurations to test
    weight_configs = [
        {"w_api": 0.0,   "label": "api=0.0"},
        {"w_api": 0.1,   "label": "api=0.1 (Baseline)"},
        {"w_api": 0.3,   "label": "api=0.3"},
        {"w_api": 0.5, "label": "api=0.5"},
    ]
    
    # 3. DP Execution loop
    results = {}
    baseline_indices = []
    n_target = min(config.slicing.target_slices, len(anchors))
    
    logger.info(f"Targeting {n_target} slices out of {len(anchors)} tag anchors.\n")
    
    print("-" * 80)
    print(f"{'Weight Config':<20} | {'w_lines, w_files':<20} | {'Mean Index Shift':<18} | {'Slices CV':<10}")
    print("-" * 80)
    
    # Run Baseline first to determine baseline_hashes
    baseline_cfg = next(c for c in weight_configs if c["w_api"] == 0.1)
    weight_configs.remove(baseline_cfg)
    weight_configs.insert(0, baseline_cfg) # Move baseline to front
    
    for cfg in weight_configs:
        w_api = cfg["w_api"]
        # Symmetric remainder
        w_sym = (1.0 - w_api) / 2.0
        
        # Override config weights
        config.slicing.distance_weights.api_break = w_api
        config.slicing.distance_weights.lines = w_sym
        config.slicing.distance_weights.files = w_sym
        
        # Normalise and calculate distance for these weights
        normalised = normalize_tag_pair_metrics(pair_metrics, config.slicing)
        distances = [m.distance for m in normalised]
        
        # Run DP selection
        selected_indices = select_tag_slices_dp(
            anchors,
            distances,
            n_target,
            gain_func=config.slicing.segment_gain,
            force_first=config.slicing.force_first_release_tag,
        )
        
        # Calculate CV
        cv = compute_segment_distance_cv(normalised, selected_indices)
        
        # Compare to baseline
        if cfg["w_api"] == 0.1:
            baseline_indices = selected_indices
            shift = 0.0 # Self
        else:
            shift = mean_index_shift(baseline_indices, selected_indices)
            
        # Log result
        label = cfg["label"]
        w_sym_str = f"{w_sym:.3f}"
        print(f"{label:<20} | {w_sym_str:<20} | {shift:<18.4f} | {cv:<10.4f}")

    print("-" * 80)
    print("Ablation finished successfully.")


def main():
    parser = argparse.ArgumentParser(description="Distance weights sensitivity analysis.")
    parser.add_argument(
        "--repo", 
        type=str, 
        required=True,
        help="Path to the cloned git repository to analyse."
    )
    parser.add_argument(
        "--config", 
        type=str,
        default="config.yaml",
        help="Path to pipeline configuration file."
    )
    args = parser.parse_args()

    repo_path = Path(args.repo)
    if not repo_path.exists() or not repo_path.is_dir():
        logger.error(f"Repository not found at: {repo_path}")
        sys.exit(1)
        
    config = load_config(args.config)
    
    # Silence dp_silent logger
    logging.getLogger("dp_silent").propagate = False
    
    # Temporarily silence semantic slicer loggers to make output clean
    logging.getLogger("pipeline.semantic_slicer").setLevel(logging.WARNING)

    run_ablation(repo_path, config)


if __name__ == "__main__":
    main()
