"""File discovery and orchestration for the lint engine."""
from __future__ import annotations

import ast
import fnmatch
import sys
from dataclasses import replace
from pathlib import Path
from typing import List, Sequence, Tuple

from sparkdoctor.lint.base import Diagnostic
from sparkdoctor.lint.engine import LintEngine


def discover_files(
    path: Path, exclude: Sequence[str] = ()
) -> list[Path]:
    """Return .py files to lint. Accepts a file or directory.

    Args:
        path: File or directory to scan.
        exclude: Glob patterns to exclude (matched against relative paths).
    """
    if path.is_file():
        return [path]
    if path.is_dir():
        files = sorted(path.rglob("*.py"))
        if exclude:
            files = [
                f for f in files
                if not _matches_any(f.relative_to(path), exclude)
            ]
        return files
    return []


def _matches_any(rel_path: Path, patterns: Sequence[str]) -> bool:
    """Return True if any part of rel_path matches any exclude pattern."""
    rel_str = str(rel_path)
    for pattern in patterns:
        if fnmatch.fnmatch(rel_str, pattern):
            return True
        # Also match against individual path components (e.g. "tests" matches "tests/foo.py")
        for part in rel_path.parts:
            if fnmatch.fnmatch(part, pattern):
                return True
    return False


def lint_file(path: Path, engine: LintEngine) -> list[Diagnostic]:
    """Read, parse, and lint a single file. Returns diagnostics with filename stamped."""
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        print(f"sparkdoctor: warning: could not read {path}: {exc}", file=sys.stderr)
        return []

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        print(f"sparkdoctor: warning: syntax error in {path}: {exc}", file=sys.stderr)
        return []

    source_lines = source.splitlines()
    diagnostics = engine.check(tree, source_lines)
    return [replace(d, filename=str(path)) for d in diagnostics]


def run(
    path: str | Path,
    exclude: Sequence[str] = (),
) -> tuple[list[Diagnostic], int]:
    """Discover files, lint them all, return (diagnostics, file_count)."""
    path = Path(path)
    files = discover_files(path, exclude=exclude)
    engine = LintEngine()
    all_diagnostics: list[Diagnostic] = []
    for f in files:
        all_diagnostics.extend(lint_file(f, engine))
    return all_diagnostics, len(files)
