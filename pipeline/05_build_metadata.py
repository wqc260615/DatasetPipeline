"""Aggregate snapshot artifacts into the final metadata.json."""

import argparse
import json
from pathlib import Path
from typing import Dict, Tuple


def _load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _count_files_and_languages(source_dir: Path) -> Tuple[int, Dict[str, int]]:
    file_count = 0
    languages: Dict[str, int] = {}
    if not source_dir.exists():
        return file_count, languages

    for file_path in source_dir.rglob("*"):
        if file_path.is_file():
            file_count += 1
            ext = file_path.suffix.lower()
            if ext == ".py":
                languages["python"] = languages.get("python", 0) + 1
            else:
                languages["other"] = languages.get("other", 0) + 1
    return file_count, languages


def _summarize_ast(parsed: Dict) -> Dict:
    files = parsed.get("files", {})
    functions = sum(len(info.get("functions", [])) for info in files.values())
    classes = sum(len(info.get("classes", [])) for info in files.values())
    imports = sum(len(info.get("imports", [])) for info in files.values())
    calls = sum(len(info.get("calls", [])) for info in files.values())
    loc = sum(info.get("loc", 0) for info in files.values())
    return {
        "files": len(files),
        "functions": functions,
        "classes": classes,
        "imports": imports,
        "calls": calls,
        "loc": loc,
    }


def build_metadata(snapshot_dir: str, out_path: str) -> Dict:
    snapshot_path = Path(snapshot_dir)
    out_file = Path(out_path)
    base_meta = _load_json(snapshot_path / "metadata.json")
    parsed = _load_json(snapshot_path / "parsed.json")
    diff = _load_json(snapshot_path / "diff.json")

    source_dir = snapshot_path / "source"
    file_count, languages = _count_files_and_languages(source_dir)
    ast_summary = _summarize_ast(parsed)
    loc_total = ast_summary.get("loc") or base_meta.get("loc")

    metadata = {
        "commit": base_meta.get("commit") or diff.get("commit"),
        "timestamp": base_meta.get("timestamp"),
        "message": base_meta.get("message"),
        "parent": diff.get("parent") or base_meta.get("parent"),
        "num_files": file_count,
        "loc": loc_total,
        "languages": sorted(languages.keys()) or ["python"],
        "stats": {
            "files_changed": diff.get("files_changed", []),
            "lines_added": diff.get("lines_added", 0),
            "lines_deleted": diff.get("lines_deleted", 0),
        },
        "ast_summary": ast_summary,
    }

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    return metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build aggregated metadata for a snapshot.")
    parser.add_argument("--snapshot_dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    build_metadata(args.snapshot_dir, args.out)
    print(f"Wrote metadata to {args.out}")

