"""
Module: semantic_slicer.py

Purpose: Identifies semantic evolution slices from Git commit history.

Key Functions:
- identify_slices(repo_path: str, config: Config) -> List[SemanticSlice]

Example:
    >>> from pipeline.config import load_config
    >>> config = load_config()
    >>> slices = identify_slices("/path/to/repo", config)
    >>> print(len(slices))
    12
"""

import logging
import tempfile
from datetime import datetime
from functools import cmp_to_key
from typing import Dict, List, NamedTuple, Optional, Tuple
from pathlib import Path
from git import Repo

from pipeline.models import SemanticSlice, SliceType, SliceMetadata
from pipeline.config import Config, SlicingConfig
from pipeline.commit_extractor import (
    parse_version_tag,
    get_diff_between_refs
)
from pipeline.slicer.semver_utils import (
    compare_prerelease_identifiers as _compare_prerelease_identifiers,
    compare_version_tags as _compare_version_tags,
)
from pipeline.slicer.distance_metrics import (
    percentile_rank as _percentile_rank,
    normalize_tag_pair_metrics as _normalize_tag_pair_metrics_impl,
)
from pipeline.slicer.dp_selector import (
    select_tag_slices_dp as _select_tag_slices_dp_impl,
)

logger = logging.getLogger(__name__)

# Cache for parsed API symbols keyed by blob hash
_api_symbol_cache: Dict[str, Dict[str, Dict[Tuple, Tuple]]] = {}

# Tag-distance slicing data types

class TagAnchor(NamedTuple):
    """Lightweight representation of a release-tag anchor point."""
    tag_name: str
    commit_hash: str
    commit_date: datetime
    version_info: dict  # output of parse_version_tag()


class TagPairMetrics(NamedTuple):
    """Raw metrics between two adjacent tag anchors."""
    from_anchor: TagAnchor
    to_anchor: TagAnchor
    delta_lines: int
    delta_files: int
    api_break: int  # 0 or 1


class NormalizedTagPairMetrics(NamedTuple):
    """Normalized metrics + computed distance for an adjacent tag pair."""
    from_anchor: TagAnchor
    to_anchor: TagAnchor
    delta_lines: int
    delta_files: int
    api_break: int
    norm_lines: float
    norm_files: float
    distance: float


def identify_slices(repo_path: str, config: Config) -> List[SemanticSlice]:
    """
    Identify semantic evolution slices from a repository.

    Args:
        repo_path: Path to Git repository
        config: Configuration object

    Returns:
        List of SemanticSlice objects
    """
    logger.info(
        f"Identifying slices for repository: {repo_path} (strategy=tag_distance_dp)"
    )
    return _identify_slices_impl(repo_path, config)


def compare_api_symbols_between_commits(
    repo: Repo,
    old_hash: str,
    new_hash: str,
    file_paths: Optional[List[str]] = None
) -> Tuple[bool, Dict[str, object]]:
    details: Dict[str, object] = {
        "matched": False,
        "checked": False,
        "changed_files": 0,
        "code_files": 0,
        "symbol_diff": False,
        "skipped_reason": None
    }
    try:
        paths: List[str] = []
        if file_paths is None:
            diff = get_diff_between_refs(repo, old_hash, new_hash)
            if not diff:
                details["skipped_reason"] = "no_diff"
                return False, details
            for item in diff:
                if item.a_path:
                    paths.append(item.a_path)
                if item.b_path and item.b_path != item.a_path:
                    paths.append(item.b_path)
            paths = list(set(paths))
        else:
            paths = file_paths
        details["changed_files"] = len(paths)
        code_files = [
            p for p in paths if p.endswith(".py") or p.endswith(".java")
        ]
        details["code_files"] = len(code_files)
        if not code_files:
            details["skipped_reason"] = "no_code_files"
            return False, details
        details["checked"] = True
        for file_path in code_files:
            lang = "python" if file_path.endswith(".py") else "java"
            old_symbols = _get_public_api_symbols(repo, old_hash, file_path, lang)
            new_symbols = _get_public_api_symbols(repo, new_hash, file_path, lang)
            if _has_symbol_diff(new_symbols, old_symbols):
                details["symbol_diff"] = True
                details["matched"] = True
                return True, details
        return False, details
    except Exception as e:
        logger.warning(f"Error comparing API symbols between {old_hash[:8]} and {new_hash[:8]}: {e}")
        details["skipped_reason"] = "error"
        return False, details


def _get_public_api_symbols(repo: Repo, commit_hash: str, file_path: str, language: str) -> Dict[str, Dict[Tuple, Tuple]]:
    """
    Extract public API symbols from a file at a specific commit.
    Returns dict with keys: "functions", "classes" mapping to signature dicts.
    """
    blob_hash = _get_blob_hash(repo, commit_hash, file_path)
    if not blob_hash:
        return {"functions": {}, "classes": {}}

    cached = _api_symbol_cache.get(blob_hash)
    if cached:
        return cached

    content = _get_file_content(repo, commit_hash, file_path)
    if content is None:
        return {"functions": {}, "classes": {}}

    symbols = _extract_public_symbols_from_content(content, file_path, language)
    _api_symbol_cache[blob_hash] = symbols
    return symbols


def _get_blob_hash(repo: Repo, commit_hash: str, file_path: str) -> Optional[str]:
    try:
        return repo.git.rev_parse(f"{commit_hash}:{file_path}").strip()
    except Exception:
        return None


def _get_file_content(repo: Repo, commit_hash: str, file_path: str) -> Optional[bytes]:
    try:
        text = repo.git.show(f"{commit_hash}:{file_path}")
        return text.encode("utf-8", errors="ignore")
    except Exception:
        return None


def _extract_public_symbols_from_content(content: bytes, file_path: str, language: str) -> Dict[str, Dict[Tuple, Tuple]]:
    from pipeline.ast_parser import parse_file

    with tempfile.NamedTemporaryFile(suffix=Path(file_path).suffix, delete=True) as tmp:
        tmp.write(content)
        tmp.flush()
        ast_data = parse_file(tmp.name, language)

    if not ast_data:
        return {"functions": {}, "classes": {}}

    public_classes = set()
    class_signatures: Dict[Tuple, Tuple] = {}
    for cls in ast_data.get("classes", []):
        name = cls.get("name")
        if not name:
            continue
        if language == "python" and name.startswith("_"):
            continue
        key = (cls.get("kind"), name)
        signature = (tuple(cls.get("base_classes", [])), tuple(cls.get("implemented_interfaces", [])))
        class_signatures[key] = signature
        public_classes.add(name)

    func_signatures: Dict[Tuple, Tuple] = {}
    for func in ast_data.get("functions", []):
        name = func.get("name")
        if not name:
            continue
        container = func.get("container")

        if language == "python":
            if name.startswith("_"):
                continue
            if container and container.startswith("_"):
                continue
        elif language == "java":
            if func.get("visibility") != "public":
                continue

        key = (func.get("kind"), container, name)
        signature = (tuple(func.get("parameters", [])), func.get("return_type"))
        func_signatures[key] = signature

    return {"functions": func_signatures, "classes": class_signatures}


def _has_symbol_diff(current: Dict[str, Dict[Tuple, Tuple]], previous: Dict[str, Dict[Tuple, Tuple]]) -> bool:
    curr_funcs = current.get("functions", {})
    prev_funcs = previous.get("functions", {})
    curr_classes = current.get("classes", {})
    prev_classes = previous.get("classes", {})

    # Function changes: add/remove/signature change
    if set(curr_funcs.keys()) != set(prev_funcs.keys()):
        return True
    for key, sig in curr_funcs.items():
        if prev_funcs.get(key) != sig:
            return True

    # Class changes: add/remove/base/interface change
    if set(curr_classes.keys()) != set(prev_classes.keys()):
        return True
    for key, sig in curr_classes.items():
        if prev_classes.get(key) != sig:
            return True

    return False


# Tag-Distance + DP slicing strategy
def _identify_slices_impl(repo_path: str, config: Config) -> List[SemanticSlice]:
    """
    Tag-Distance + DP slicing strategy.

    1. Collect release-tag anchors reachable from the main branch.
    2. Compute semantic distance for each adjacent tag pair.
    3. Normalise metrics via percentile rank.
    4. Select *N* tag-anchors that maximise total segment gain (DP).
    5. Convert selected anchors to ``SemanticSlice`` objects.

    Args:
        repo_path: Path to Git repository
        config: Pipeline configuration

    Returns:
        List of SemanticSlice objects
    """
    try:
        repo = Repo(repo_path)
        slicing = config.slicing

        # Step 1 – collect tag anchors
        anchors = collect_tag_anchors(repo, slicing)
        if not anchors:
            logger.warning("No tag anchors found for slicing strategy")
            return []

        logger.info(f"Collected {len(anchors)} tag anchors (sorted by SemVer)")

        # Step 2 – compute raw metrics for each adjacent pair
        pair_metrics = compute_adjacent_tag_metrics(repo, anchors, slicing)
        logger.info(
            f"Computed metrics for {len(pair_metrics)} adjacent tag pairs"
        )

        # Step 3 – normalise and compute distances
        normalised = normalize_tag_pair_metrics(pair_metrics, slicing)
        distances = [m.distance for m in normalised]

        # Step 4 – DP selection
        n_target = min(slicing.target_slices, len(anchors))
        selected_indices = select_tag_slices_dp(
            anchors,
            distances,
            n_target,
            gain_func=slicing.segment_gain,
            force_first=slicing.force_first_release_tag,
        )
        logger.info(
            f"DP selected {len(selected_indices)} anchors out of {len(anchors)} "
            f"(target={slicing.target_slices})"
        )

        # Log each selected anchor
        for rank, idx in enumerate(selected_indices, 1):
            a = anchors[idx]
            logger.info(
                f"  #{rank}: tag={a.tag_name}  commit={a.commit_hash[:8]}  "
                f"date={a.commit_date.isoformat()}"
            )

        # Step 5 – build SemanticSlice objects
        # Pre-compute a lookup: anchor_index → left-segment normalised metrics
        seg_lookup = _build_segment_lookup(normalised, anchors, selected_indices)

        slices: List[SemanticSlice] = []
        for idx in selected_indices:
            anchor = anchors[idx]
            try:
                slice_obj = _create_slice_from_anchor(
                    anchor, repo, config, seg_lookup.get(idx)
                )
                if slice_obj:
                    slices.append(slice_obj)
            except Exception as e:
                logger.warning(
                    f"Error creating slice for tag {anchor.tag_name}: {e}"
                )
                continue

        # Sort by commit date (should already be in order)
        slices.sort(key=lambda s: s.commit_date)
        logger.info(f"Final slice count: {len(slices)}")
        return slices

    except Exception as e:
        logger.error(f"Error in slicing for {repo_path}: {e}", exc_info=True)
        return []


# Collect tag anchors
def collect_tag_anchors(
    repo: Repo,
    config: SlicingConfig,
) -> List[TagAnchor]:
    """
    Collect semver tag anchors from the repository, optionally filtering to
    those reachable from the main branch.

    Returns a list sorted by SemVer precedence (ascending).
    """
    # Determine main branch head (for main_only filtering)
    main_head_hash: Optional[str] = None
    if config.tag_scope == "main_only":
        main_head_hash = _resolve_main_branch(repo, config.main_branch_name)
        if main_head_hash:
            logger.info(
                f"Main branch resolved to {config.main_branch_name} "
                f"({main_head_hash[:8]})"
            )
        else:
            logger.warning(
                "Could not resolve main branch – falling back to tag_scope='all'"
            )

    raw_anchors: Dict[str, TagAnchor] = {}  # keyed by commit_hash

    for tag_ref in repo.tags:
        tag_name = tag_ref.name
        version_info = parse_version_tag(tag_name)

        # Optionally skip non-semver tags
        if version_info is None:
            if config.filter_non_semver:
                continue
            else:
                # Keep unknown tags with a synthetic version_info so they sort
                # after all real semver tags (gives them lowest selection
                # priority in DP, but doesn't discard information).
                version_info = {
                    "major": 999999, "minor": 999999, "patch": 999999,
                    "prerelease": tag_name, "build": None,
                    "type": "unknown",
                }

        try:
            tag_commit = tag_ref.commit
        except Exception:
            logger.debug(f"Skipping tag {tag_name}: cannot resolve commit")
            continue

        commit_hash = tag_commit.hexsha

        # main_only reachability check
        if main_head_hash is not None:
            if not _is_ancestor(repo, commit_hash, main_head_hash):
                logger.debug(
                    f"Skipping tag {tag_name}: not reachable from main branch"
                )
                continue

        commit_date = tag_commit.committed_datetime

        # De-duplicate: keep the tag with the *highest* semver when multiple
        # tags point at the same commit.
        existing = raw_anchors.get(commit_hash)
        if existing is not None:
            try:
                # Only compare if both are real semver
                if (
                    existing.version_info.get("type") != "unknown"
                    and version_info.get("type") != "unknown"
                ):
                    if _compare_version_tags(tag_name, existing.tag_name) <= 0:
                        continue
                elif version_info.get("type") == "unknown":
                    # Existing is real semver, new is unknown – keep existing
                    continue
            except ValueError:
                continue

        raw_anchors[commit_hash] = TagAnchor(
            tag_name=tag_name,
            commit_hash=commit_hash,
            commit_date=commit_date,
            version_info=version_info,
        )

    anchors = list(raw_anchors.values())

    # Use the proper SemVer comparator for real tags
    semver_anchors = [a for a in anchors if a.version_info.get("type") != "unknown"]
    unknown_anchors = [a for a in anchors if a.version_info.get("type") == "unknown"]

    if semver_anchors:
        semver_anchors.sort(
            key=cmp_to_key(
                lambda a, b: _compare_version_tags(a.tag_name, b.tag_name)
            )
        )

    # Unknown tags appended at the end sorted by commit date
    unknown_anchors.sort(key=lambda a: a.commit_date)

    sorted_anchors = semver_anchors + unknown_anchors
    logger.debug(
        f"Tag anchors after filtering & sorting: "
        f"{[a.tag_name for a in sorted_anchors]}"
    )
    return sorted_anchors


def _resolve_main_branch(repo: Repo, preferred_name: str) -> Optional[str]:
    """Return the HEAD commit hash of the main branch, or *None*."""
    for name in (preferred_name, "master", "main"):
        try:
            return repo.commit(name).hexsha
        except Exception:
            continue
    # Last resort: try the repo HEAD
    try:
        return repo.head.commit.hexsha
    except Exception:
        return None


def _is_ancestor(repo: Repo, maybe_ancestor: str, descendant: str) -> bool:
    """Check if *maybe_ancestor* is an ancestor of *descendant*."""
    try:
        repo.git.merge_base("--is-ancestor", maybe_ancestor, descendant)
        return True
    except Exception:
        return False


# Compute adjacent tag-pair metrics
def compute_adjacent_tag_metrics(
    repo: Repo,
    anchors: List[TagAnchor],
    config: SlicingConfig,
) -> List[TagPairMetrics]:
    """
    For each pair of adjacent anchors ``(t_i, t_{i+1})`` compute:

    - ``delta_lines``: total added + deleted lines (via ``git diff --numstat``)
    - ``delta_files``: number of changed file entries
    - ``api_break``:   whether a public-API symbol diff was detected (0/1)
    """
    metrics: List[TagPairMetrics] = []

    for i in range(len(anchors) - 1):
        a_from = anchors[i]
        a_to = anchors[i + 1]

        try:
            delta_lines, delta_files = _diff_numstat(
                repo, a_from.commit_hash, a_to.commit_hash
            )
        except Exception as e:
            logger.warning(
                f"Error computing diff between {a_from.tag_name} and "
                f"{a_to.tag_name}: {e} – using zeros"
            )
            delta_lines, delta_files = 0, 0

        # API break detection (reuse existing symbol diff machinery)
        api_break = 0
        try:
            has_break, _details = compare_api_symbols_between_commits(
                repo, a_from.commit_hash, a_to.commit_hash
            )
            if has_break:
                api_break = 1
        except Exception as e:
            logger.warning(
                f"API break detection failed between {a_from.tag_name} and "
                f"{a_to.tag_name}: {e} – defaulting to 0 (api_break_status=unknown)"
            )

        m = TagPairMetrics(
            from_anchor=a_from,
            to_anchor=a_to,
            delta_lines=delta_lines,
            delta_files=delta_files,
            api_break=api_break,
        )
        metrics.append(m)

        logger.debug(
            f"  {a_from.tag_name} → {a_to.tag_name}: "
            f"ΔLines={delta_lines}, ΔFiles={delta_files}, APIBreak={api_break}"
        )

    return metrics


def _diff_numstat(
    repo: Repo, old_hash: str, new_hash: str
) -> Tuple[int, int]:
    """
    Run ``git diff --numstat`` between two refs.

    Returns ``(total_lines_changed, file_count)``.
    Binary files (reported as ``-\t-``) are counted as 0 lines.
    """
    raw = repo.git.diff("--numstat", old_hash, new_hash)
    if not raw.strip():
        return 0, 0

    total_lines = 0
    file_count = 0
    for line in raw.strip().split("\n"):
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        added_str, deleted_str, _path = parts[0], parts[1], parts[2]
        file_count += 1
        # Binary files are reported as "-"
        if added_str == "-" or deleted_str == "-":
            continue
        total_lines += int(added_str) + int(deleted_str)

    return total_lines, file_count


def normalize_tag_pair_metrics(
    metrics: List[TagPairMetrics],
    config: SlicingConfig,
) -> List[NormalizedTagPairMetrics]:
    return _normalize_tag_pair_metrics_impl(
        metrics,
        config,
        NormalizedTagPairMetrics,
    )


def select_tag_slices_dp(
    anchors: List[TagAnchor],
    distances: List[float],
    n: int,
    gain_func: str = "log1p",
    force_first: bool = True,
) -> List[int]:
    return _select_tag_slices_dp_impl(
        anchors,
        distances,
        n,
        gain_func=gain_func,
        force_first=force_first,
        logger=logger,
    )


# Helper: build SemanticSlice from a TagAnchor
def _create_slice_from_anchor(
    anchor: TagAnchor,
    repo: Repo,
    config: Config,
    segment_info: Optional[Dict[str, object]] = None,
) -> Optional[SemanticSlice]:
    """
    Build a ``SemanticSlice`` from a tag anchor.

    Constructs the slice directly from tag anchor data.
    """
    try:
        repo_name = Path(repo.working_dir).name
        date_str = anchor.commit_date.strftime("%Y%m%d")
        slice_id = f"{repo_name}_{anchor.commit_hash[:8]}_{date_str}"

        # Get commit stats for metadata
        git_commit = repo.commit(anchor.commit_hash)
        stats = git_commit.stats.total
        lines_added = stats.get("insertions", 0)
        lines_deleted = stats.get("deletions", 0)
        files_changed = stats.get("files", 0)

        # Score breakdown carries the slicing audit trail
        score_breakdown: Dict[str, object] = {
            "strategy": "tag_distance_dp",
            "tag_name": anchor.tag_name,
            "version_info": anchor.version_info,
        }
        if segment_info:
            score_breakdown["segment"] = segment_info

        metadata = SliceMetadata(
            total_files=0,  # Will be populated by enrich_slice_with_files
            total_lines=0,  # Will be populated by enrich_slice_with_files
            target_language_total_files=0,  # Will be populated by enrich_slice_with_files
            target_language_total_lines=0,  # Will be populated by enrich_slice_with_files
            changed_files_since_prev_slice=files_changed,
            commit_message=git_commit.message.strip(),
            lines_added=lines_added,
            lines_deleted=lines_deleted,
            files_modified=None,  # populated later by enrich_slice_with_files
            slice_score=segment_info.get("distance", 0.0) if segment_info else 0.0,
            score_breakdown=score_breakdown,
        )

        return SemanticSlice(
            slice_id=slice_id,
            commit_hash=anchor.commit_hash,
            commit_date=anchor.commit_date.isoformat(),
            slice_type=SliceType.VERSION_RELEASE,
            version_tag=anchor.tag_name,
            files=[],  # populated later by enrich_slice_with_files
            metadata=metadata,
        )

    except Exception as e:
        logger.error(
            f"Error creating slice for tag {anchor.tag_name}: {e}"
        )
        return None


def _build_segment_lookup(
    normalised: List[NormalizedTagPairMetrics],
    anchors: List[TagAnchor],
    selected_indices: List[int],
) -> Dict[int, Dict[str, object]]:
    """
    For each selected anchor (except the first), build an audit dict
    describing the segment from its predecessor in the selection.

    Returns ``{anchor_index: segment_info_dict}``.
    """
    # Pre-compute prefix-sum of distances keyed by anchor index
    # normalised[i] covers anchors[i] → anchors[i+1]
    dist_by_idx: Dict[int, float] = {}
    for i, nm in enumerate(normalised):
        dist_by_idx[i] = nm.distance

    lookup: Dict[int, Dict[str, object]] = {}

    for pos in range(len(selected_indices)):
        idx = selected_indices[pos]
        if pos == 0:
            # First selected anchor – no preceding segment
            lookup[idx] = {
                "position_in_selection": 0,
                "is_first": True,
                "distance": 0.0,
            }
            continue

        prev_idx = selected_indices[pos - 1]
        # Accumulate distances between prev_idx and idx
        seg_dist = sum(
            dist_by_idx.get(k, 0.0) for k in range(prev_idx, idx)
        )
        # Accumulate raw deltas
        seg_lines = sum(
            normalised[k].delta_lines for k in range(prev_idx, idx)
            if k < len(normalised)
        )
        seg_files = sum(
            normalised[k].delta_files for k in range(prev_idx, idx)
            if k < len(normalised)
        )
        seg_api = max(
            (normalised[k].api_break for k in range(prev_idx, idx)
             if k < len(normalised)),
            default=0,
        )

        lookup[idx] = {
            "position_in_selection": pos,
            "is_first": False,
            "prev_tag": anchors[prev_idx].tag_name,
            "distance": seg_dist,
            "delta_lines": seg_lines,
            "delta_files": seg_files,
            "api_break": seg_api,
        }

    return lookup
