"""Context retrieval: fetch source file content from a specific commit in a repo."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Optional


class ContextRetriever:
    """Retrieves source file content at a given semantic slice via git."""

    # Subtypes that require content from multiple files (cross-file relationship queries)
    _CROSS_FILE_SUBTYPES = frozenset({"class_subclasses", "class_instantiation_sites"})

    # Evolution subtypes whose boundary commits often predate/postdate the function's
    # lifetime; a nearest-existing-snapshot fallback is used for these.
    _EVOLUTION_SUBTYPES = frozenset({
        "function_signature_evolution",
        "function_return_type_evolution",
    })

    # Version subtypes that ask for the exact introduction / removal version.
    # The two boundary snapshots are almost always wrong for these (the file either
    # doesn't exist yet at the from-commit, has been deleted before the to-commit,
    # or has been moved to a different path).  A dedicated slice-range scan is used
    # instead to locate the true boundary snapshot.
    _VERSION_SUBTYPES = frozenset({
        "function_first_introduced",
        "function_last_present",
        "class_first_introduced",
        "class_last_present",
    })

    def __init__(self, slices_root: Path, repos_root: Path, max_chars: int = 16000):
        self.slices_root = slices_root
        self.repos_root = repos_root
        self.max_chars = max_chars
        self._index: dict[str, dict[str, dict]] = {}
        self._symbol_cache: dict[tuple, dict] = {}

    def _load_index(self, repo: str) -> None:
        if repo in self._index:
            return
        repo_slices_dir = self.slices_root / repo / "slices"
        self._index[repo] = {}
        for meta_file in sorted(repo_slices_dir.glob("*/metadata.json")):
            data = json.loads(meta_file.read_text())
            self._index[repo][data["slice_id"]] = {
                "commit_hash": data["commit_hash"],
                "version_tag": data.get("version_tag"),
                "slice_dir": meta_file.parent,
            }

    def _load_slice_symbols(self, repo: str, slice_id: str) -> dict:
        """Load and cache AST symbol data (classes, functions) for a slice."""
        cache_key = (repo, slice_id)
        if cache_key in self._symbol_cache:
            return self._symbol_cache[cache_key]
        self._load_index(repo)
        entry = self._index.get(repo, {}).get(slice_id, {})
        slice_dir: Optional[Path] = entry.get("slice_dir")
        result: dict = {"classes": [], "functions": []}
        if slice_dir:
            for key, fname in [("classes", "classes.json"), ("functions", "functions.json")]:
                sym_path = slice_dir / "symbols" / fname
                if sym_path.exists():
                    result[key] = json.loads(sym_path.read_text())
        self._symbol_cache[cache_key] = result
        return result

    def _get_related_file_paths(self, qa_pair: dict, symbols: dict) -> list:
        """Return related file paths needed for cross-file intrinsic subtypes."""
        subtype = qa_pair.get("qa_subtype", "")
        evidence = qa_pair.get("evidence", {})
        class_name = evidence.get("name", "")
        primary_file = evidence.get("file_path", "")
        related: list = []

        if subtype == "class_subclasses":
            for cls in symbols.get("classes", []):
                if class_name in (cls.get("base_classes") or []):
                    fp = cls.get("file_path", "")
                    if fp and fp != primary_file and fp not in related:
                        related.append(fp)
        elif subtype == "class_instantiation_sites":
            for func in symbols.get("functions", []):
                if class_name in (func.get("instantiations") or []):
                    fp = func.get("file_path", "")
                    if fp and fp != primary_file and fp not in related:
                        related.append(fp)
        return related

    def _get_sorted_slice_ids(self, repo: str) -> list:
        """Return all slice IDs for *repo* sorted chronologically (by YYYYMMDD suffix)."""
        self._load_index(repo)
        return sorted(
            self._index[repo].keys(),
            key=lambda sid: sid.rsplit("_", 1)[-1],
        )

    def _find_content_fallback(
        self,
        repo: str,
        sorted_ids: list,
        anchor_id: str,
        file_path: str,
        target_symbol: Optional[str],
        direction: str,
        max_tries: int = 8,
    ) -> tuple:
        """Scan slices from *anchor_id* to find the nearest one where the file exists.

        *direction* is ``"forward"`` (later slices) or ``"backward"`` (earlier slices).
        Caps at *max_tries* git calls to bound latency.
        Returns ``(content, version_tag)`` or ``(None, None)``.
        """
        if anchor_id not in sorted_ids:
            return None, None
        start = sorted_ids.index(anchor_id)
        indices = (
            range(start, len(sorted_ids))
            if direction == "forward"
            else range(start, -1, -1)
        )
        for i, idx in enumerate(indices):
            if i >= max_tries:
                break
            sid = sorted_ids[idx]
            entry = self._index[repo][sid]
            content = self.get_file_content(
                repo, entry["commit_hash"], file_path, target_symbol
            )
            if content is not None:
                return content, entry.get("version_tag") or sid
        return None, None

    def _find_version_boundary(
        self,
        repo: str,
        from_slice_id: str,
        to_slice_id: str,
        file_path: str,
        target_symbol: Optional[str],
        subtype: str,
    ) -> tuple:
        """Scan the slice range to locate the exact boundary snapshot for version subtypes.

        For ``*_first_introduced`` subtypes: scan **forward** from *from_slice_id*
        and return the first slice where the file exists (the introduction point).

        For ``*_last_present`` subtypes: scan **backward** from *to_slice_id* and
        return the last slice where the file exists (the removal / rename point).

        Unlike ``_find_content_fallback``, this method scans the *entire* range
        (not capped) because the answer version may be far from either boundary.

        Returns ``(content, version_tag)`` or ``(None, None)`` when nothing is found.
        """
        sorted_ids = self._get_sorted_slice_ids(repo)
        if from_slice_id not in sorted_ids or to_slice_id not in sorted_ids:
            return None, None
        from_idx = sorted_ids.index(from_slice_id)
        to_idx = sorted_ids.index(to_slice_id)
        range_ids = sorted_ids[from_idx : to_idx + 1]

        candidates = (
            range_ids
            if "first_introduced" in subtype
            else list(reversed(range_ids))
        )
        for sid in candidates:
            entry = self._index[repo][sid]
            content = self.get_file_content(
                repo, entry["commit_hash"], file_path, target_symbol
            )
            if content is not None:
                return content, entry.get("version_tag") or sid
        return None, None

    def get_commit_hash(self, repo: str, slice_id: str) -> Optional[str]:
        self._load_index(repo)
        entry = self._index.get(repo, {}).get(slice_id)
        return entry["commit_hash"] if entry else None

    def get_version_tag(self, repo: str, slice_id: str) -> Optional[str]:
        self._load_index(repo)
        entry = self._index.get(repo, {}).get(slice_id)
        return entry["version_tag"] if entry else None

    @staticmethod
    def _extract_around_symbol(content: str, symbol: str, window: int) -> str:
        """Return a window of *window* chars centred on the line defining *symbol*.

        *symbol* may be 'ClassName.method_name' or just 'func_name'.
        We search for the innermost name (e.g. 'delete' from 'FastAPI.delete')
        as a `def <name>(` pattern.  If not found, fall back to plain truncation.
        """
        method = symbol.split(".")[-1]
        search = f"def {method}("
        idx = content.find(search)
        if idx == -1:
            return content[:window] + "\n# [truncated]"
        start = max(0, idx - window // 4)
        end = min(len(content), idx + window * 3 // 4)
        prefix = "# [...leading content omitted...]\n" if start > 0 else ""
        suffix = "\n# [truncated]" if end < len(content) else ""
        return prefix + content[start:end] + suffix

    def get_file_content(
        self,
        repo: str,
        commit_hash: str,
        file_path: str,
        target_symbol: Optional[str] = None,
    ) -> Optional[str]:
        """Fetch file content at a specific commit via `git show`.

        If *target_symbol* is given and the full file exceeds *max_chars*,
        extract a window around the symbol definition instead of truncating
        from the top.
        """
        repo_path = self.repos_root / repo
        try:
            result = subprocess.run(
                ["git", "show", f"{commit_hash}:{file_path}"],
                capture_output=True,
                text=True,
                cwd=repo_path,
                timeout=15,
            )
            if result.returncode != 0:
                return None
            content = result.stdout
            if len(content) > self.max_chars:
                if target_symbol:
                    content = self._extract_around_symbol(
                        content, target_symbol, self.max_chars
                    )
                else:
                    content = content[: self.max_chars] + "\n# [truncated]"
            return content
        except Exception:
            return None

    def get_context_for_qa(self, qa_pair: dict) -> dict:
        """Build a context dict for a QA pair.

        For intrinsic/extrinsic:
            {"content": str|None, "version": str|None, "file_path": str}

        For temporal:
            {"from_content": str|None, "to_content": str|None,
             "from_version": str|None, "to_version": str|None, "file_path": str}
        """
        repo = qa_pair["repo"]
        evidence = qa_pair["evidence"]
        file_path = evidence["file_path"]
        target_symbol: Optional[str] = evidence.get("name")
        qa_type = qa_pair["qa_type"]
        qa_subtype = qa_pair.get("qa_subtype", "")

        if qa_type == "temporal":
            from_slice_id = qa_pair["from_slice_id"]
            to_slice_id = qa_pair["to_slice_id"]

            # Version subtypes require locating the exact introduction / removal
            # slice within the range.  The two boundary commits are almost always
            # the wrong snapshots (file not yet created, already deleted, or moved
            # to a new path).  Scan the full slice range instead.
            if qa_subtype in self._VERSION_SUBTYPES:
                content, version = self._find_version_boundary(
                    repo, from_slice_id, to_slice_id, file_path, target_symbol, qa_subtype
                )
                return {
                    "from_content": content,
                    "to_content": None,
                    "from_version": version,
                    "to_version": None,
                    "file_path": file_path,
                }

            from_hash = self.get_commit_hash(repo, from_slice_id)
            to_hash = self.get_commit_hash(repo, to_slice_id)

            from_content = (
                self.get_file_content(repo, from_hash, file_path, target_symbol)
                if from_hash
                else None
            )
            to_content = (
                self.get_file_content(repo, to_hash, file_path, target_symbol)
                if to_hash
                else None
            )

            from_version = self.get_version_tag(repo, from_slice_id) or from_slice_id
            to_version = self.get_version_tag(repo, to_slice_id) or to_slice_id

            # Evolution subtypes span the full slice chain; the boundary commits
            # often predate or postdate the function's lifetime.  Scan forward /
            # backward to find the nearest snapshot where the file actually exists,
            # capped to avoid excessive git calls (token budget stays bounded).
            if qa_subtype in self._EVOLUTION_SUBTYPES:
                sorted_ids = self._get_sorted_slice_ids(repo)
                if from_content is None:
                    fc, fv = self._find_content_fallback(
                        repo, sorted_ids, from_slice_id, file_path, target_symbol, "forward"
                    )
                    if fc is not None:
                        from_content, from_version = fc, fv
                if to_content is None:
                    tc, tv = self._find_content_fallback(
                        repo, sorted_ids, to_slice_id, file_path, target_symbol, "backward"
                    )
                    if tc is not None:
                        to_content, to_version = tc, tv

            return {
                "from_content": from_content,
                "to_content": to_content,
                "from_version": from_version,
                "to_version": to_version,
                "file_path": file_path,
            }
        else:
            slice_id = qa_pair["slice_id"]
            commit_hash = self.get_commit_hash(repo, slice_id) if slice_id else None
            content = (
                self.get_file_content(repo, commit_hash, file_path, target_symbol)
                if commit_hash
                else None
            )
            version = (
                self.get_version_tag(repo, slice_id) or slice_id if slice_id else None
            )
            related_contents: list = []
            if qa_subtype in self._CROSS_FILE_SUBTYPES and slice_id and commit_hash:
                symbols = self._load_slice_symbols(repo, slice_id)
                related_paths = self._get_related_file_paths(qa_pair, symbols)
                per_file_budget = max(2000, self.max_chars // max(len(related_paths), 1))
                for rpath in related_paths[:6]:  # cap at 6 related files
                    rcontent = self.get_file_content(repo, commit_hash, rpath)
                    if rcontent:
                        related_contents.append({
                            "file_path": rpath,
                            "content": rcontent[:per_file_budget],
                        })
            return {
                "content": content,
                "version": version,
                "file_path": file_path,
                "related_contents": related_contents,
            }
