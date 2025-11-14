# python pipeline/03_parse_snapshot.py --snapshot_dir ./data/snapshots/<commit> --out ./data/snapshots/<commit>/parsed.json

import argparse
import ast
import json
from pathlib import Path
from typing import Dict, List, Optional

from tqdm import tqdm

IGNORED_DIRS = {"venv", ".tox", "node_modules"}


def _call_name(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parts: List[str] = []
        current = node
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def parse_python_file(path: Path) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        src = f.read()
    loc = sum(1 for line in src.splitlines() if line.strip())
    try:
        tree = ast.parse(src)
    except Exception as exc:
        return {"error": str(exc), "loc": loc}

    functions = []
    classes = []
    imports: List[Dict] = []
    calls: List[Dict] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            sig = {
                "name": node.name,
                "lineno": node.lineno,
                "end_lineno": getattr(node, "end_lineno", None),
                "args": [arg.arg for arg in node.args.args],
                "returns": ast.unparse(node.returns) if getattr(node, "returns", None) else None,
                "docstring": ast.get_docstring(node),
            }
            functions.append(sig)
        elif isinstance(node, ast.ClassDef):
            classes.append(
                {
                    "name": node.name,
                    "lineno": node.lineno,
                    "docstring": ast.get_docstring(node),
                }
            )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imports.append({"module": alias.name, "alias": alias.asname, "lineno": node.lineno})
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                imports.append(
                    {"module": f"{module}.{alias.name}".strip("."), "alias": alias.asname, "lineno": node.lineno}
                )
        elif isinstance(node, ast.Call):
            name = _call_name(node.func)
            if name:
                calls.append({"target": name, "lineno": getattr(node, "lineno", None)})

    return {"functions": functions, "classes": classes, "imports": imports, "calls": calls, "loc": loc}


def _should_skip(path: Path) -> bool:
    return any(part in IGNORED_DIRS for part in path.parts)


def parse_snapshot(snapshot_dir: str) -> Dict:
    snapshot_path = Path(snapshot_dir)
    source_dir = snapshot_path / "source"
    root = source_dir if source_dir.exists() else snapshot_path
    parsed = {"files": {}}
    py_files = [p for p in root.rglob("*.py") if not _should_skip(p.relative_to(root))]

    for pyfile in tqdm(py_files, desc="Parsing Python files"):
        rel = str(pyfile.relative_to(root))
        parsed["files"][rel] = parse_python_file(pyfile)
    parsed["summary"] = {
        "file_count": len(py_files),
        "loc": sum(info.get("loc", 0) for info in parsed["files"].values()),
    }
    return parsed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse Python AST summaries for a snapshot directory.")
    parser.add_argument("--snapshot_dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    parsed = parse_snapshot(args.snapshot_dir)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2, ensure_ascii=False)
    print(f"Wrote parsed snapshot to {args.out}")
