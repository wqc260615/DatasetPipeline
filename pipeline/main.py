"""
Main entry point for the dataset pipeline.

Processes repositories to extract semantic evolution slices.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List

from pipeline.config import load_config, get_default_config
from pipeline.repository_cloner import clone_repository, validate_repository
from pipeline.semantic_slicer import identify_slices
from pipeline.metadata_generator import enrich_slice_with_files
from pipeline.validation.slice_validator import validate_all_slices
from pipeline.models import RepositoryDataset, RepositoryInfo
from pipeline.output_writer import save_repository_dataset
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def process_repository(
    repo_url: str,
    config,
    output_dir: str,
    existing_repo_action: str = "ask"
) -> bool:
    """
    Process a single repository end-to-end.
    
    Args:
        repo_url: Repository URL
        config: Configuration object
        output_dir: Output directory for results
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Processing repository: {repo_url}")
    
    try:
        # Extract repo name from URL
        repo_name = repo_url.split('/')[-1].replace('.git', '')
        repo_dir = Path(config.storage.repositories_dir) / repo_name
        
        # Clone repository
        logger.info("Cloning repository...")
        cloned_path = clone_repository(
            repo_url,
            str(repo_dir),
            existing_repo_action=existing_repo_action
        )
        
        if not cloned_path:
            logger.error(f"Failed to clone repository: {repo_url}")
            return False
        
        # Validate repository
        if not validate_repository(cloned_path):
            logger.error(f"Invalid repository: {repo_url}")
            return False
        
        # Identify slices
        logger.info("Identifying semantic slices...")
        slices = identify_slices(cloned_path, config)
        
        if not slices:
            logger.warning(f"No slices identified for repository: {repo_url}")
            return False
        
        # Enrich slices with file information
        logger.info(f"Enriching {len(slices)} slices with file information...")
        enriched_slices = []
        for i, slice in enumerate(slices, 1):
            logger.info(f"Processing slice {i}/{len(slices)}: {slice.slice_id}")
            try:
                enriched = enrich_slice_with_files(slice, cloned_path, config)
                enriched_slices.append(enriched)
            except Exception as e:
                logger.error(f"Error enriching slice {slice.slice_id}: {e}")
                continue
        
        # Validate slices
        logger.info("Validating slices...")
        validation_results = validate_all_slices(enriched_slices, config.validation)
        logger.info(f"Validation results: {validation_results['valid_slices']}/{validation_results['total_slices']} valid")
        
        # Create repository info
        repo_info = RepositoryInfo(
            name=repo_name,
            url=repo_url,
            language="unknown",  # Could be detected from files
            clone_date=datetime.now().isoformat()
        )
        
        # Create dataset
        dataset = RepositoryDataset(
            repository=repo_info,
            slices=enriched_slices
        )
        
        # Save to structured directory layout
        output_path = Path(output_dir)
        save_repository_dataset(dataset, output_path)
        
        logger.info(f"Successfully processed repository: {repo_url}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing repository {repo_url}: {e}", exc_info=True)
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract semantic evolution slices from Git repositories"
    )
    parser.add_argument(
        "--repo-url",
        type=str,
        help="Single repository URL to process"
    )
    parser.add_argument(
        "--repo-list",
        type=str,
        help="Path to file containing repository URLs (one per line)"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/slices",
        help="Output directory for results"
    )
    parser.add_argument(
        "--existing-repo-action",
        type=str,
        choices=["ask", "update", "skip"],
        default=None,
        help="When repository already exists locally: ask/update/skip (overrides config)"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        logger.warning(f"Config file not found: {args.config}, using defaults")
        config = get_default_config()
    
    # Setup logging from config
    logging.getLogger().setLevel(getattr(logging, config.logging.level))

    existing_repo_action = args.existing_repo_action or config.storage.existing_repo_action
    
    # Get repository URLs
    repo_urls = []
    if args.repo_url:
        repo_urls.append(args.repo_url)
    elif args.repo_list:
        with open(args.repo_list, 'r') as f:
            repo_urls = [line.strip() for line in f if line.strip()]
    else:
        logger.error("Must provide either --repo-url or --repo-list")
        sys.exit(1)
    
    # Process repositories
    success_count = 0
    for repo_url in repo_urls:
        if process_repository(repo_url, config, args.output_dir, existing_repo_action):
            success_count += 1
    
    logger.info(f"Processed {success_count}/{len(repo_urls)} repositories successfully")
    
    if success_count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
