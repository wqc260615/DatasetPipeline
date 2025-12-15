"""Track entity lifecycle across repository versions.

This module builds a timeline of when functions, classes, and other entities
were introduced, modified, or removed across commits. This data is essential
for answering RQ1 questions like "When was class X introduced?" or
"Did function Y exist in version Z?"
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Entity types we track
ENTITY_TYPES = ["function", "class"]


def _make_entity_key(file_path: str, entity_type: str, name: str) -> str:
    """Create a unique key for an entity."""
    return f"{entity_type}::{file_path}::{name}"


def _extract_entities_from_parsed(parsed: Dict) -> Dict[str, Dict]:
    """Extract all entities from a parsed snapshot.
    
    Returns a dict mapping entity_key -> entity_info
    """
    entities: Dict[str, Dict] = {}
    
    for file_path, file_info in parsed.get("files", {}).items():
        if "error" in file_info:
            continue
        
        # Extract functions
        for func in file_info.get("functions", []):
            key = _make_entity_key(file_path, "function", func["name"])
            entities[key] = {
                "type": "function",
                "name": func["name"],
                "file": file_path,
                "lineno": func.get("lineno"),
                "end_lineno": func.get("end_lineno"),
                "args": func.get("args", []),
                "returns": func.get("returns"),
                "docstring": func.get("docstring"),
                "signature": _build_signature(func),
            }
        
        # Extract classes
        for cls in file_info.get("classes", []):
            key = _make_entity_key(file_path, "class", cls["name"])
            entities[key] = {
                "type": "class",
                "name": cls["name"],
                "file": file_path,
                "lineno": cls.get("lineno"),
                "docstring": cls.get("docstring"),
            }
    
    return entities


def _build_signature(func: Dict) -> str:
    """Build a function signature string for comparison."""
    args = ", ".join(func.get("args", []))
    returns = func.get("returns")
    if returns:
        return f"{func['name']}({args}) -> {returns}"
    return f"{func['name']}({args})"


def _compare_entities(old: Dict, new: Dict) -> Dict:
    """Compare two entity dicts to detect changes."""
    changes = {
        "signature_changed": old.get("signature") != new.get("signature"),
        "args_changed": old.get("args") != new.get("args"),
        "returns_changed": old.get("returns") != new.get("returns"),
        "docstring_changed": old.get("docstring") != new.get("docstring"),
        "lineno_changed": old.get("lineno") != new.get("lineno"),
    }
    changes["modified"] = any(changes.values())
    return changes


def build_entity_timeline(
    snapshots_dir: str,
    index_path: str,
    output_path: str,
) -> Dict:
    """Build a timeline of entity changes across all snapshots.
    
    This creates:
    1. entity_index: Maps each entity to its lifecycle events
    2. version_entities: Maps each commit to the entities present in that version
    3. entity_events: Chronological list of all introduction/removal/modification events
    """
    snapshots_path = Path(snapshots_dir)
    
    # Load commit index to get chronological order
    with open(index_path, "r", encoding="utf-8") as f:
        index_data = json.load(f)
    
    # Sort commits chronologically (oldest first for timeline construction)
    commits = sorted(index_data["commits"], key=lambda c: c["timestamp"])
    
    # Data structures to build
    entity_index: Dict[str, Dict] = {}  # entity_key -> lifecycle info
    version_entities: Dict[str, List[str]] = {}  # commit_hash -> [entity_keys]
    entity_events: List[Dict] = []  # chronological list of events
    
    # Track previous state for comparison
    prev_entities: Dict[str, Dict] = {}
    prev_commit: Optional[str] = None
    
    for commit in commits:
        commit_hash = commit["hash"]
        timestamp = commit["timestamp"]
        tags = commit.get("tags", [])
        
        # Load parsed data for this snapshot
        parsed_path = snapshots_path / commit_hash / "parsed.json"
        if not parsed_path.exists():
            continue
        
        with open(parsed_path, "r", encoding="utf-8") as f:
            parsed = json.load(f)
        
        current_entities = _extract_entities_from_parsed(parsed)
        current_keys = set(current_entities.keys())
        prev_keys = set(prev_entities.keys())
        
        # Store version -> entities mapping
        version_entities[commit_hash] = list(current_keys)
        
        # Detect introduced entities (new in current)
        introduced = current_keys - prev_keys
        for key in introduced:
            entity = current_entities[key]
            event = {
                "event": "introduced",
                "entity_key": key,
                "entity_type": entity["type"],
                "entity_name": entity["name"],
                "file": entity["file"],
                "commit": commit_hash,
                "timestamp": timestamp,
                "tags": tags,
                "signature": entity.get("signature"),
            }
            entity_events.append(event)
            
            # Initialize entity lifecycle
            entity_index[key] = {
                "entity_key": key,
                "type": entity["type"],
                "name": entity["name"],
                "introduced_in": commit_hash,
                "introduced_at": timestamp,
                "introduced_tags": tags,
                "removed_in": None,
                "removed_at": None,
                "last_seen_in": commit_hash,
                "last_seen_at": timestamp,
                "files": [entity["file"]],
                "modifications": [],
                "exists_in_versions": [commit_hash],
            }
        
        # Detect removed entities
        removed = prev_keys - current_keys
        for key in removed:
            event = {
                "event": "removed",
                "entity_key": key,
                "commit": commit_hash,
                "timestamp": timestamp,
                "tags": tags,
            }
            entity_events.append(event)
            
            if key in entity_index:
                entity_index[key]["removed_in"] = commit_hash
                entity_index[key]["removed_at"] = timestamp
        
        # Detect modifications to existing entities
        common = current_keys & prev_keys
        for key in common:
            changes = _compare_entities(prev_entities[key], current_entities[key])
            if changes["modified"]:
                event = {
                    "event": "modified",
                    "entity_key": key,
                    "commit": commit_hash,
                    "timestamp": timestamp,
                    "tags": tags,
                    "changes": changes,
                    "old_signature": prev_entities[key].get("signature"),
                    "new_signature": current_entities[key].get("signature"),
                }
                entity_events.append(event)
                
                if key in entity_index:
                    entity_index[key]["modifications"].append({
                        "commit": commit_hash,
                        "timestamp": timestamp,
                        "changes": changes,
                    })
            
            # Update last seen
            if key in entity_index:
                entity_index[key]["last_seen_in"] = commit_hash
                entity_index[key]["last_seen_at"] = timestamp
                entity_index[key]["exists_in_versions"].append(commit_hash)
                
                # Track file moves
                curr_file = current_entities[key]["file"]
                if curr_file not in entity_index[key]["files"]:
                    entity_index[key]["files"].append(curr_file)
        
        prev_entities = current_entities
        prev_commit = commit_hash
    
    # Build summary statistics
    summary = {
        "total_entities_tracked": len(entity_index),
        "total_events": len(entity_events),
        "functions_tracked": sum(1 for e in entity_index.values() if e["type"] == "function"),
        "classes_tracked": sum(1 for e in entity_index.values() if e["type"] == "class"),
        "commits_processed": len(version_entities),
    }
    
    result = {
        "summary": summary,
        "entity_index": entity_index,
        "version_entities": version_entities,
        "entity_events": entity_events,
    }
    
    # Write output
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    return result


def query_entity_existence(
    entity_timeline: Dict,
    entity_type: str,
    entity_name: str,
    version: Optional[str] = None,
    file_hint: Optional[str] = None,
) -> Dict:
    """Query whether an entity exists and its lifecycle information.
    
    This supports RQ1 questions like:
    - "Does function X exist in version Y?"
    - "When was class Z introduced?"
    """
    entity_index = entity_timeline.get("entity_index", {})
    
    # Find matching entities
    matches = []
    for key, info in entity_index.items():
        if info["type"] == entity_type and info["name"] == entity_name:
            if file_hint and file_hint not in key:
                continue
            matches.append(info)
    
    if not matches:
        return {"found": False, "entity_name": entity_name, "entity_type": entity_type}
    
    # If version is specified, check existence in that version
    if version:
        for match in matches:
            if version in match.get("exists_in_versions", []):
                return {
                    "found": True,
                    "exists_in_version": True,
                    "version": version,
                    **match,
                }
        return {
            "found": True,
            "exists_in_version": False,
            "version": version,
            "matches": matches,
        }
    
    return {
        "found": True,
        "matches": matches,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build entity timeline across versions.")
    parser.add_argument("--snapshots_dir", required=True, help="Directory containing snapshots")
    parser.add_argument("--index_path", required=True, help="Path to index.json")
    parser.add_argument("--out", required=True, help="Output path for entity timeline JSON")
    args = parser.parse_args()
    
    result = build_entity_timeline(args.snapshots_dir, args.index_path, args.out)
    print(f"Tracked {result['summary']['total_entities_tracked']} entities across {result['summary']['commits_processed']} commits")
    print(f"Wrote entity timeline to {args.out}")



