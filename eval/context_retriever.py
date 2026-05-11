"""Context retrieval: fetch source file content from a specific commit in a repo."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Optional


class ContextRetriever:
    """Retrieves source file content at a given semantic slice via git."""

    def __init__(self, slices_root: Path, repos_root: Path, max_chars: int = 16000):
        self.slices_root = slices_root
        self.repos_root = repos_root
        self.max_chars = max_chars
        # repo -> {slice_id -> {"commit_hash": str, "version_tag": str | None}}
        self._index: dict[str, dict[str, dict]] = {}

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
            }

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
        # Find the earliest `def <method>(` line
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

        if qa_type == "temporal":
            from_slice_id = qa_pair["from_slice_id"]
            to_slice_id = qa_pair["to_slice_id"]

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

            return {
                "from_content": from_content,
                "to_content": to_content,
                "from_version": (
                    self.get_version_tag(repo, from_slice_id) or from_slice_id
                ),
                "to_version": (
                    self.get_version_tag(repo, to_slice_id) or to_slice_id
                ),
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
            return {
                "content": content,
                "version": version,
                "file_path": file_path,
            }
