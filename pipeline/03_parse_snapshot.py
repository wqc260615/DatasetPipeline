# python pipeline/03_parse_snapshot.py --snapshot_dir ./data/snapshots/<commit> --out ./data/snapshots/<commit>/parsed.json

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set

from tqdm import tqdm

from parsers import detect_language, get_parser, get_supported_languages

IGNORED_DIRS = {"venv", ".tox", "node_modules", ".git", "__pycache__", "node_modules", "target", "build", "dist"}


def _should_skip(path: Path) -> bool:
    """Check if a file path should be skipped."""
    return any(part in IGNORED_DIRS for part in path.parts)


def _get_supported_extensions() -> Set[str]:
    """Get all file extensions supported by registered parsers."""
    extensions = set()
    for lang in get_supported_languages():
        parser = get_parser(lang)
        if parser:
            extensions.update(parser.get_file_extensions())
    return extensions


def parse_snapshot(
    snapshot_dir: str,
    languages: Optional[List[str]] = None,
    include_unsupported: bool = False,
) -> Dict:
    """Parse all supported source files in a snapshot directory.
    
    Args:
        snapshot_dir: Path to snapshot directory
        languages: Optional list of languages to parse (e.g., ['python', 'javascript']).
                   If None, parses all supported languages.
        include_unsupported: If True, includes files with unsupported languages
                            (with error markers). Default False.
    
    Returns:
        Dictionary with parsed file information
    """
    snapshot_path = Path(snapshot_dir)
    source_dir = snapshot_path / "source"
    root = source_dir if source_dir.exists() else snapshot_path
    
    parsed = {"files": {}}
    language_stats = defaultdict(int)
    file_extensions = _get_supported_extensions()
    
    # Find all source files
    all_files = []
    for ext in file_extensions:
        pattern = f"*.{ext}"
        all_files.extend(root.rglob(pattern))
    
    # Filter and group by language
    files_by_lang: Dict[str, List[Path]] = defaultdict(list)
    unsupported_files: List[Path] = []
    
    for file_path in all_files:
        if _should_skip(file_path):
            continue
        
        rel = file_path.relative_to(root)
        language = detect_language(str(rel))
        
        if language:
            parser = get_parser(language)
            if parser and (languages is None or language in languages):
                files_by_lang[language].append(file_path)
            elif include_unsupported:
                unsupported_files.append(file_path)
        elif include_unsupported:
            unsupported_files.append(file_path)
    
    # Parse files by language
    total_files = sum(len(files) for files in files_by_lang.values())
    if include_unsupported:
        total_files += len(unsupported_files)
    
    with tqdm(total=total_files, desc="Parsing files") as pbar:
        for language, files in files_by_lang.items():
            parser = get_parser(language)
            if not parser:
                continue
            
            for file_path in files:
                rel = str(file_path.relative_to(root))
                try:
                    parsed_file = parser.parse_file(file_path)
                    parsed["files"][rel] = parsed_file.to_dict()
                    language_stats[language] += 1
                except Exception as exc:
                    parsed["files"][rel] = {
                        "error": f"Parse error: {str(exc)}",
                        "functions": [],
                        "classes": [],
                        "imports": [],
                        "calls": [],
                        "loc": 0,
                    }
                pbar.update(1)
        
        # Handle unsupported files if requested
        if include_unsupported:
            for file_path in unsupported_files:
                rel = str(file_path.relative_to(root))
                parsed["files"][rel] = {
                    "error": "Language not supported",
                    "functions": [],
                    "classes": [],
                    "imports": [],
                    "calls": [],
                    "loc": 0,
                }
                pbar.update(1)
    
    # Build summary
    total_loc = sum(info.get("loc", 0) for info in parsed["files"].values())
    parsed["summary"] = {
        "file_count": len(parsed["files"]),
        "loc": total_loc,
        "languages": dict(language_stats),
        "supported_languages": get_supported_languages(),
    }
    
    return parsed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse source code AST summaries for a snapshot directory (multi-language support)."
    )
    parser.add_argument("--snapshot_dir", required=False, help="Path to snapshot directory")
    parser.add_argument("--out", required=False, help="Output path for parsed JSON")
    parser.add_argument(
        "--languages",
        nargs="*",
        help="Specific languages to parse (e.g., python javascript). If not specified, parses all supported languages.",
    )
    parser.add_argument(
        "--include-unsupported",
        action="store_true",
        help="Include files with unsupported languages (marked with errors)",
    )
    parser.add_argument(
        "--list-languages",
        action="store_true",
        help="List all supported languages and exit",
    )
    args = parser.parse_args()
    
    if args.list_languages:
        languages = get_supported_languages()
        print("Supported languages:")
        for lang in languages:
            parser_obj = get_parser(lang)
            if parser_obj:
                exts = ", ".join(f".{ext}" for ext in parser_obj.get_file_extensions())
                print(f"  - {lang}: {exts}")
        sys.exit(0)
    
    if not args.snapshot_dir or not args.out:
        parser.error("--snapshot_dir and --out are required unless --list-languages is used")
    
    parsed = parse_snapshot(
        args.snapshot_dir,
        languages=args.languages,
        include_unsupported=args.include_unsupported,
    )
    
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2, ensure_ascii=False)
    
    print(f"Wrote parsed snapshot to {args.out}")
    print(f"Parsed {parsed['summary']['file_count']} files")
    print(f"Languages: {', '.join(parsed['summary']['languages'].keys())}")
    print(f"Total LOC: {parsed['summary']['loc']}")
