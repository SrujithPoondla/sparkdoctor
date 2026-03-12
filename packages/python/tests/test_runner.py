"""Tests for file discovery, exclude logic, and lint_file error handling."""

import logging
from pathlib import Path

from sparkdoctor.lint.engine import LintEngine
from sparkdoctor.lint.runner import _matches_any, discover_files, lint_file

# ── discover_files ──────────────────────────────────────────────────────────


def test_discover_single_file(tmp_path):
    f = tmp_path / "job.py"
    f.write_text("x = 1")
    assert discover_files(f) == [f]


def test_discover_directory(tmp_path):
    (tmp_path / "a.py").write_text("x = 1")
    (tmp_path / "b.py").write_text("x = 2")
    (tmp_path / "readme.md").write_text("# hi")
    files = discover_files(tmp_path)
    assert len(files) == 2
    assert all(f.suffix == ".py" for f in files)


def test_discover_nonexistent():
    assert discover_files(Path("/nonexistent/path")) == []


def test_discover_with_exclude(tmp_path):
    (tmp_path / "job.py").write_text("x = 1")
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_job.py").write_text("x = 2")
    files = discover_files(tmp_path, exclude=("tests",))
    assert len(files) == 1
    assert files[0].name == "job.py"


# ── _matches_any ────────────────────────────────────────────────────────────


def test_matches_full_path():
    assert _matches_any(Path("tests/test_foo.py"), ("tests/test_foo.py",))


def test_matches_component():
    assert _matches_any(Path("tests/test_foo.py"), ("tests",))


def test_matches_glob():
    assert _matches_any(Path("tests/test_foo.py"), ("test_*",))


def test_no_match():
    assert not _matches_any(Path("src/main.py"), ("tests",))


# ── lint_file error paths ──────────────────────────────────────────────────


def test_lint_file_syntax_error(tmp_path, caplog):
    f = tmp_path / "bad_syntax.py"
    f.write_text("def foo(\n")
    engine = LintEngine()
    with caplog.at_level(logging.WARNING):
        result = lint_file(f, engine)
    assert result == []
    assert "syntax error" in caplog.text


def test_lint_file_nonexistent(tmp_path, caplog):
    f = tmp_path / "missing.py"
    engine = LintEngine()
    with caplog.at_level(logging.WARNING):
        result = lint_file(f, engine)
    assert result == []
    assert "could not read" in caplog.text


def test_lint_file_stamps_filename(tmp_path):
    f = tmp_path / "job.py"
    f.write_text("df.repartition(200)")
    engine = LintEngine()
    results = lint_file(f, engine)
    assert len(results) >= 1
    assert all(d.filename == str(f) for d in results)
