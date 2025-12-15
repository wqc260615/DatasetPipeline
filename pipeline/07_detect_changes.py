"""Detect and classify code evolution events between consecutive versions.

This module identifies specific change types for RQ2 evaluation:
- Rename: Entity name changed but structure similar
- Move: Entity relocated to different file
- Signature Change: Function parameters or return type modified
- Refactoring: Structural changes within entity body

The detection uses AST-based comparison and heuristics for matching entities
across versions (similar to RefDiff's approach).
"""

import argparse
import difflib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


# Change type classification
class ChangeType:
    ADDED = "added"
    REMOVED = "removed"
    RENAMED = "renamed"
    MOVED = "moved"
    SIGNATURE_CHANGED = "signature_changed"
    BODY_MODIFIED = "body_modified"
    DOCSTRING_CHANGED = "docstring_changed"


def _similarity_ratio(a: str, b: str) -> float:
    """Calculate string similarity using SequenceMatcher."""
    return difflib.SequenceMatcher(None, a, b).ratio()


def _extract_entities_from_snapshot(parsed: Dict) -> Tuple[Dict[str, Dict], Dict[str, Set[str]]]:
    """Extract entities and build lookup structures.
    
    Returns:
        entities: dict mapping unique_key -> entity_info
        name_to_keys: dict mapping simple_name -> set of unique_keys
    """
    entities: Dict[str, Dict] = {}
    name_to_keys: Dict[str, Set[str]] = {}
    
    for file_path, file_info in parsed.get("files", {}).items():
        if "error" in file_info:
            continue
        
        for func in file_info.get("functions", []):
            key = f"function::{file_path}::{func['name']}"
            entities[key] = {
                "type": "function",
                "name": func["name"],
                "file": file_path,
                "lineno": func.get("lineno"),
                "args": func.get("args", []),
                "returns": func.get("returns"),
                "docstring": func.get("docstring"),
            }
            name_to_keys.setdefault(func["name"], set()).add(key)
        
        for cls in file_info.get("classes", []):
            key = f"class::{file_path}::{cls['name']}"
            entities[key] = {
                "type": "class",
                "name": cls["name"],
                "file": file_path,
                "lineno": cls.get("lineno"),
                "docstring": cls.get("docstring"),
            }
            name_to_keys.setdefault(cls["name"], set()).add(key)
    
    return entities, name_to_keys


def _match_renamed_entity(
    removed: Dict[str, Dict],
    added: Dict[str, Dict],
    similarity_threshold: float = 0.7,
) -> List[Tuple[str, str, float]]:
    """Find potential rename matches between removed and added entities.
    
    Uses a combination of:
    - Name similarity
    - Signature similarity (for functions)
    - File path similarity
    """
    matches = []
    
    for old_key, old_info in removed.items():
        best_match = None
        best_score = 0.0
        
        for new_key, new_info in added.items():
            if old_info["type"] != new_info["type"]:
                continue
            
            # Calculate composite similarity score
            name_sim = _similarity_ratio(old_info["name"], new_info["name"])
            file_sim = _similarity_ratio(old_info["file"], new_info["file"])
            
            # For functions, also compare arguments
            if old_info["type"] == "function":
                old_args = ",".join(old_info.get("args", []))
                new_args = ",".join(new_info.get("args", []))
                args_sim = _similarity_ratio(old_args, new_args)
                score = (name_sim * 0.4 + file_sim * 0.3 + args_sim * 0.3)
            else:
                score = (name_sim * 0.5 + file_sim * 0.5)
            
            if score > best_score and score >= similarity_threshold:
                best_score = score
                best_match = new_key
        
        if best_match:
            matches.append((old_key, best_match, best_score))
    
    return matches


def detect_changes_between_versions(
    old_parsed: Dict,
    new_parsed: Dict,
    old_commit: str,
    new_commit: str,
    old_timestamp: str,
    new_timestamp: str,
) -> Dict:
    """Detect and classify all changes between two consecutive versions.
    
    Returns structured change data suitable for RQ2 evaluation.
    """
    old_entities, old_names = _extract_entities_from_snapshot(old_parsed)
    new_entities, new_names = _extract_entities_from_snapshot(new_parsed)
    
    old_keys = set(old_entities.keys())
    new_keys = set(new_entities.keys())
    
    # Initial classification
    pure_added = new_keys - old_keys
    pure_removed = old_keys - new_keys
    common = old_keys & new_keys
    
    changes: List[Dict] = []
    entity_mappings: Dict[str, str] = {}  # old_key -> new_key for renamed/moved
    
    # Detect renames and moves among added/removed pairs
    removed_entities = {k: old_entities[k] for k in pure_removed}
    added_entities = {k: new_entities[k] for k in pure_added}
    
    rename_matches = _match_renamed_entity(removed_entities, added_entities)
    matched_old = set()
    matched_new = set()
    
    for old_key, new_key, confidence in rename_matches:
        matched_old.add(old_key)
        matched_new.add(new_key)
        entity_mappings[old_key] = new_key
        
        old_info = old_entities[old_key]
        new_info = new_entities[new_key]
        
        # Determine if rename, move, or both
        is_rename = old_info["name"] != new_info["name"]
        is_move = old_info["file"] != new_info["file"]
        
        if is_rename and is_move:
            change_type = "renamed_and_moved"
        elif is_rename:
            change_type = ChangeType.RENAMED
        else:
            change_type = ChangeType.MOVED
        
        changes.append({
            "change_type": change_type,
            "entity_type": old_info["type"],
            "old_key": old_key,
            "new_key": new_key,
            "old_name": old_info["name"],
            "new_name": new_info["name"],
            "old_file": old_info["file"],
            "new_file": new_info["file"],
            "confidence": confidence,
            "old_commit": old_commit,
            "new_commit": new_commit,
        })
    
    # True additions (not matched to any removal)
    for key in pure_added - matched_new:
        info = new_entities[key]
        changes.append({
            "change_type": ChangeType.ADDED,
            "entity_type": info["type"],
            "entity_key": key,
            "name": info["name"],
            "file": info["file"],
            "commit": new_commit,
            "timestamp": new_timestamp,
        })
    
    # True removals (not matched to any addition)
    for key in pure_removed - matched_old:
        info = old_entities[key]
        changes.append({
            "change_type": ChangeType.REMOVED,
            "entity_type": info["type"],
            "entity_key": key,
            "name": info["name"],
            "file": info["file"],
            "commit": new_commit,
            "timestamp": new_timestamp,
        })
    
    # Detect modifications in common entities
    for key in common:
        old_info = old_entities[key]
        new_info = new_entities[key]
        
        modifications = []
        
        # Check signature changes (for functions)
        if old_info["type"] == "function":
            if old_info.get("args") != new_info.get("args"):
                modifications.append("args_changed")
            if old_info.get("returns") != new_info.get("returns"):
                modifications.append("returns_changed")
        
        # Check docstring changes
        if old_info.get("docstring") != new_info.get("docstring"):
            modifications.append("docstring_changed")
        
        if modifications:
            changes.append({
                "change_type": ChangeType.SIGNATURE_CHANGED if "args_changed" in modifications or "returns_changed" in modifications else ChangeType.DOCSTRING_CHANGED,
                "entity_type": old_info["type"],
                "entity_key": key,
                "name": old_info["name"],
                "file": old_info["file"],
                "modifications": modifications,
                "old_args": old_info.get("args"),
                "new_args": new_info.get("args"),
                "old_returns": old_info.get("returns"),
                "new_returns": new_info.get("returns"),
                "old_commit": old_commit,
                "new_commit": new_commit,
            })
    
    # Build summary
    change_counts = {}
    for change in changes:
        ct = change["change_type"]
        change_counts[ct] = change_counts.get(ct, 0) + 1
    
    return {
        "old_commit": old_commit,
        "new_commit": new_commit,
        "old_timestamp": old_timestamp,
        "new_timestamp": new_timestamp,
        "summary": {
            "total_changes": len(changes),
            "change_counts": change_counts,
            "entities_mapped": len(entity_mappings),
        },
        "changes": changes,
        "entity_mappings": entity_mappings,
    }


def build_change_history(
    snapshots_dir: str,
    index_path: str,
    output_path: str,
) -> Dict:
    """Build a complete change history for all consecutive version pairs.
    
    This creates the ground truth data needed for RQ2:
    - Change events classified by type
    - Entity mappings for tracking across versions
    - Per-change-type statistics
    """
    snapshots_path = Path(snapshots_dir)
    
    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)
    
    # Sort chronologically
    commits = sorted(index_data["commits"], key=lambda c: c["timestamp"])
    
    all_changes: List[Dict] = []
    global_change_counts: Dict[str, int] = {}
    global_entity_mappings: Dict[str, str] = {}
    
    prev_commit = None
    prev_parsed = None
    prev_timestamp = None
    
    for commit in commits:
        commit_hash = commit["hash"]
        timestamp = commit["timestamp"]
        parsed_path = snapshots_path / commit_hash / "parsed.json"
        
        if not parsed_path.exists():
            continue
        
        with open(parsed_path, "r", encoding="utf-8") as f:
            parsed = json.load(f)
        
        if prev_commit is not None and prev_parsed is not None:
            change_data = detect_changes_between_versions(
                prev_parsed, parsed,
                prev_commit, commit_hash,
                prev_timestamp, timestamp,
            )
            all_changes.append(change_data)
            
            # Aggregate counts
            for ct, count in change_data["summary"]["change_counts"].items():
                global_change_counts[ct] = global_change_counts.get(ct, 0) + count
            
            # Merge entity mappings
            global_entity_mappings.update(change_data["entity_mappings"])
        
        prev_commit = commit_hash
        prev_parsed = parsed
        prev_timestamp = timestamp
    
    result = {
        "summary": {
            "total_version_pairs": len(all_changes),
            "global_change_counts": global_change_counts,
            "total_entity_mappings": len(global_entity_mappings),
        },
        "change_history": all_changes,
        "entity_mappings": global_entity_mappings,
    }
    
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    return result


def get_change_pair(
    change_history: Dict,
    entity_key: str,
) -> Optional[Dict]:
    """Get the before/after pair for a specific entity change.
    
    This supports RQ2's minimal change pair construction.
    """
    for version_change in change_history.get("change_history", []):
        for change in version_change.get("changes", []):
            if change.get("entity_key") == entity_key or change.get("old_key") == entity_key:
                return {
                    "before_commit": version_change["old_commit"],
                    "after_commit": version_change["new_commit"],
                    "change": change,
                }
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect code evolution events between versions.")
    parser.add_argument("--snapshots_dir", required=True, help="Directory containing snapshots")
    parser.add_argument("--index_path", required=True, help="Path to index.json")
    parser.add_argument("--out", required=True, help="Output path for change history JSON")
    args = parser.parse_args()
    
    result = build_change_history(args.snapshots_dir, args.index_path, args.out)
    print(f"Detected changes across {result['summary']['total_version_pairs']} version pairs")
    print(f"Change distribution: {result['summary']['global_change_counts']}")
    print(f"Wrote change history to {args.out}")



