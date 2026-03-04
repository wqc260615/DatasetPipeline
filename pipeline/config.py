"""
Configuration management for the pipeline.

Loads and validates configuration from config.yaml.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class DistanceWeightsConfig(BaseModel):
    """Weights for tag-pair semantic distance computation."""
    lines: float = Field(default=0.45, description="Weight for normalized ΔLines")
    files: float = Field(default=0.45, description="Weight for normalized ΔFiles")
    api_break: float = Field(default=0.10, description="Weight for API break indicator")


class ApiBreakConfig(BaseModel):
    """Backend configuration for API break detection."""
    python_backend: str = Field(default="ast_symbol_diff", description="Python API break detection backend")
    java_backend: str = Field(default="refactoring_miner", description="Java API break detection backend")


class SlicingConfig(BaseModel):
    """Configuration for semantic slicing."""
    # --- Tag-Distance + DP fields ---
    target_slices: int = Field(default=20, description="Target number of slices to select (budget N)")
    tag_scope: str = Field(
        default="main_only",
        description="Tag filtering scope: 'main_only' | 'all'"
    )
    main_branch_name: str = Field(
        default="main",
        description="Main branch name; auto-fallback to 'master' if not found"
    )
    distance_weights: DistanceWeightsConfig = Field(
        default_factory=DistanceWeightsConfig,
        description="Weights for tag-pair semantic distance"
    )
    segment_gain: str = Field(
        default="log1p",
        description="Gain function for DP segment scoring: 'log1p' | 'sqrt'"
    )
    force_first_release_tag: bool = Field(
        default=True,
        description="Force the first release tag to be selected"
    )
    filter_non_semver: bool = Field(
        default=False,
        description="Exclude tags that cannot be parsed as semver"
    )
    min_days_between_selected: int = Field(
        default=0,
        description="Optional minimum days between selected tag slices (0 = no constraint)"
    )
    api_break: ApiBreakConfig = Field(
        default_factory=ApiBreakConfig,
        description="API break detection backend configuration"
    )


class ParsingConfig(BaseModel):
    """Configuration for code parsing."""
    languages: List[str] = Field(default=["python", "java"])
    timeout_seconds: int = Field(default=30, description="Timeout for parsing operations")
    supported_extensions: Dict[str, List[str]] = Field(
        default_factory=lambda: {
            "python": [".py"],
            "java": [".java"]
        }
    )


class StorageConfig(BaseModel):
    """Configuration for data storage."""
    output_dir: str = Field(default="./data/slices", description="Output directory for slices")
    cache_dir: str = Field(default="./data/cache", description="Cache directory")
    repositories_dir: str = Field(default="./data/repositories", description="Directory for cloned repositories")


class RepositorySelectionConfig(BaseModel):
    """Configuration for repository selection."""
    min_commits: int = Field(default=100, description="Minimum commits required")
    max_commits: int = Field(default=50000, description="Maximum commits allowed")
    min_commits_per_year: int = Field(default=10, description="Minimum commits per year")
    required_languages: List[str] = Field(default=["python", "java"])
    library_percentage: float = Field(default=0.6, description="Target percentage of library repositories")
    application_percentage: float = Field(default=0.4, description="Target percentage of application repositories")
    permissive_licenses: List[str] = Field(
        default=["MIT", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause"]
    )


class ValidationConfig(BaseModel):
    """Configuration for validation."""
    min_code_files_per_slice: int = Field(default=1, description="Minimum code files per slice")
    ast_parsing_success_rate_threshold: float = Field(
        default=0.9, description="Minimum AST parsing success rate"
    )
    enable_build_check: bool = Field(default=False, description="Enable build/compilation checks")


class LoggingConfig(BaseModel):
    """Configuration for logging."""
    level: str = Field(default="INFO", description="Logging level")
    format: str = Field(
        default="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        description="Log format string"
    )


class Config(BaseModel):
    """Main configuration model."""
    slicing: SlicingConfig
    parsing: ParsingConfig
    storage: StorageConfig
    repository_selection: RepositorySelectionConfig
    validation: ValidationConfig
    logging: LoggingConfig


def load_config(config_path: str = "config.yaml") -> Config:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Config object with validated settings
    """
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        config_dict = yaml.safe_load(f)
    
    return Config(**config_dict)


def get_default_config() -> Config:
    """Get default configuration."""
    return Config(
        slicing=SlicingConfig(),
        parsing=ParsingConfig(),
        storage=StorageConfig(),
        repository_selection=RepositorySelectionConfig(),
        validation=ValidationConfig(),
        logging=LoggingConfig()
    )
