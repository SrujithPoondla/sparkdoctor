"""File discovery and orchestration for the lint engine."""
from __future__ import annotations

import ast
import sys
from dataclasses import replace
from pathlib import Path
from typing import List, Tuple

from sparkdoctor.lint.base import Diagnostic
from sparkdoctor.lint.engine import LintEngine


def discover_files(path: Path) -> list[Path]:
    """Return .py files to lint. Accepts a file or directory."""
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(path.rglob("*.py"))
    return []


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


def run(path: str | Path) -> tuple[list[Diagnostic], int]:
    """Discover files, lint them all, return (diagnostics, file_count)."""
    path = Path(path)
    files = discover_files(path)
    engine = LintEngine()
    all_diagnostics: list[Diagnostic] = []
    for f in files:
        all_diagnostics.extend(lint_file(f, engine))
    return all_diagnostics, len(files)
