"""Integration tests: verify fixture files against all rules."""

import ast
from pathlib import Path

from sparkdoctor.lint.engine import LintEngine

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _lint_fixture(filename: str):
    path = FIXTURES_DIR / filename
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    engine = LintEngine()
    return engine.check(tree, source.splitlines())


def test_bad_job_triggers_all_rules():
    """bad_job.py must trigger all rule IDs."""
    diagnostics = _lint_fixture("bad_job.py")
    rule_ids = {d.rule_id for d in diagnostics}
    expected = {
        "SDK001",
        "SDK002",
        "SDK003",
        "SDK004",
        "SDK005",
        "SDK006",
        "SDK007",
        "SDK012",
        "SDK013",
        "SDK014",
        "SDK015",
        "SDK016",
        "SDK017",
        "SDK019",
        "SDK023",
        "SDK025",
        "SDK026",
        "SDK027",
        "SDK031",
    }
    assert expected.issubset(rule_ids), f"Missing rules: {expected - rule_ids}"


def test_clean_job_triggers_no_rules():
    """clean_job.py must produce zero diagnostics."""
    diagnostics = _lint_fixture("clean_job.py")
    assert diagnostics == [], (
        f"Expected no diagnostics but got {len(diagnostics)}: "
        f"{[d.rule_id + ':' + str(d.line) for d in diagnostics]}"
    )
