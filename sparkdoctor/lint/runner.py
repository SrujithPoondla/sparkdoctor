"""File discovery and orchestration for the lint engine."""
from __future__ import annotations

import fnmatch
import logging
from dataclasses import replace
from pathlib import Path
from typing import Sequence

from sparkdoctor.lint.base import Diagnostic, Parser
from sparkdoctor.lint.engine import LintEngine
from sparkdoctor.rules.registry import get_all_parsers

logger = logging.getLogger(__name__)


def discover_files(
    path: Path, exclude: Sequence[str] = (), parsers: dict[str, Parser] | None = None
) -> list[Path]:
    """Return lintable files. Accepts a file or directory.

    Args:
        path: File or directory to scan.
        exclude: Glob patterns to exclude (matched against relative paths).
        parsers: Language parsers — determines which file extensions to scan.
            Defaults to Python only (.py).
    """
    if parsers is None:
        parsers = get_all_parsers()

    extensions = set()
    for parser in parsers.values():
        extensions.update(parser.file_extensions)

    if path.is_file():
        if path.suffix in extensions:
            return [path]
        return []
    if path.is_dir():
        files: list[Path] = []
        for ext in sorted(extensions):
            files.extend(path.rglob(f"*{ext}"))
        files.sort()
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


def lint_file(
    path: Path,
    engine: LintEngine,
    parsers: dict[str, Parser] | None = None,
) -> list[Diagnostic]:
    """Read, parse, and lint a single file. Returns diagnostics with filename stamped."""
    if parsers is None:
        parsers = get_all_parsers()

    parser = _parser_for_file(path, parsers)
    if parser is None:
        logger.warning("no parser for file extension '%s': %s", path.suffix, path)
        return []

    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        logger.warning("could not read %s: %s", path, exc)
        return []

    try:
        tree = parser.parse(source, filename=str(path))
    except SyntaxError as exc:
        logger.warning("syntax error in %s: %s", path, exc)
        return []

    source_lines = source.splitlines()
    diagnostics = engine.check(tree, source_lines, language=parser.language)
    return [replace(d, filename=str(path)) for d in diagnostics]


def _parser_for_file(path: Path, parsers: dict[str, Parser]) -> Parser | None:
    """Find the parser that handles this file's extension."""
    for parser in parsers.values():
        if path.suffix in parser.file_extensions:
            return parser
    return None


def run(
    path: str | Path,
    exclude: Sequence[str] = (),
    disable: set[str] | None = None,
) -> tuple[list[Diagnostic], int]:
    """Discover files, lint them all, return (diagnostics, file_count)."""
    path = Path(path)
    parsers = get_all_parsers()
    files = discover_files(path, exclude=exclude, parsers=parsers)
    engine = LintEngine(disable=disable)
    all_diagnostics: list[Diagnostic] = []
    for f in files:
        all_diagnostics.extend(lint_file(f, engine, parsers=parsers))
    return all_diagnostics, len(files)
