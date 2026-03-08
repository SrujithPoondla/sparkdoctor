"""CLI smoke tests for sparkdoctor lint."""
import json
from pathlib import Path

from typer.testing import CliRunner

from sparkdoctor.cli.main import app

runner = CliRunner()
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_lint_bad_job_terminal():
    result = runner.invoke(app, ["lint", str(FIXTURES_DIR / "bad_job.py")])
    assert result.exit_code == 0
    assert "SDK001" in result.output
    assert "SDK004" in result.output


def test_lint_bad_job_json():
    result = runner.invoke(
        app, ["lint", str(FIXTURES_DIR / "bad_job.py"), "--format", "json"]
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) > 0
    rule_ids = {d["rule_id"] for d in data}
    assert "SDK001" in rule_ids


def test_lint_bad_job_exit_code():
    result = runner.invoke(
        app, ["lint", str(FIXTURES_DIR / "bad_job.py"), "--exit-code"]
    )
    assert result.exit_code == 1


def test_lint_clean_job_exit_code():
    result = runner.invoke(
        app, ["lint", str(FIXTURES_DIR / "clean_job.py"), "--exit-code"]
    )
    assert result.exit_code == 0


def test_lint_nonexistent_path():
    result = runner.invoke(app, ["lint", "/nonexistent/path.py"])
    assert result.exit_code == 2


def test_lint_severity_filter():
    result = runner.invoke(
        app,
        [
            "lint",
            str(FIXTURES_DIR / "bad_job.py"),
            "--format",
            "json",
            "--severity",
            "error",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    # All returned diagnostics should be error severity
    for d in data:
        assert d["severity"] == "error"


def test_lint_disable_single_rule():
    result = runner.invoke(
        app,
        [
            "lint",
            str(FIXTURES_DIR / "bad_job.py"),
            "--format",
            "json",
            "--disable",
            "SDK001",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    rule_ids = {d["rule_id"] for d in data}
    assert "SDK001" not in rule_ids
    # Other rules should still fire
    assert "SDK004" in rule_ids


def test_lint_disable_multiple_rules():
    result = runner.invoke(
        app,
        [
            "lint",
            str(FIXTURES_DIR / "bad_job.py"),
            "--format",
            "json",
            "--disable",
            "SDK001",
            "--disable",
            "SDK023",
            "--disable",
            "SDK025",
        ],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    rule_ids = {d["rule_id"] for d in data}
    assert "SDK001" not in rule_ids
    assert "SDK023" not in rule_ids
    assert "SDK025" not in rule_ids


def test_lint_exclude_directory(tmp_path):
    """--exclude should skip matching files."""
    # Create a mini project with a tests/ dir
    job = tmp_path / "job.py"
    job.write_text("df.repartition(200)")
    test_dir = tmp_path / "tests"
    test_dir.mkdir()
    test_file = test_dir / "test_job.py"
    test_file.write_text("df.repartition(100)")

    # Without exclude: both files scanned
    result = runner.invoke(
        app, ["lint", str(tmp_path), "--format", "json"]
    )
    data = json.loads(result.output)
    filenames = {d["filename"] for d in data}
    assert any("test_job.py" in f for f in filenames)

    # With exclude: tests/ skipped
    result = runner.invoke(
        app, ["lint", str(tmp_path), "--format", "json", "--exclude", "tests"]
    )
    data = json.loads(result.output)
    filenames = {d["filename"] for d in data}
    assert not any("test_job.py" in f for f in filenames)
    assert len(data) >= 1  # job.py still scanned
