"""Tests for inline # noqa suppression comments."""
import ast

from sparkdoctor.lint.engine import LintEngine


def lint(source: str):
    tree = ast.parse(source)
    engine = LintEngine()
    return engine.check(tree, source.splitlines())


def test_bare_noqa_suppresses_all():
    source = "df.repartition(200)  # noqa"
    results = lint(source)
    assert results == []


def test_noqa_with_specific_rule():
    source = "df.repartition(200)  # noqa: SDK001"
    results = lint(source)
    assert results == []


def test_noqa_does_not_suppress_other_rules():
    source = "df.repartition(1)  # noqa: SDK001"
    results = lint(source)
    # SDK006 should still fire (repartition(1)), SDK001 suppressed
    rule_ids = {d.rule_id for d in results}
    assert "SDK001" not in rule_ids
    assert "SDK006" in rule_ids


def test_noqa_multiple_rules():
    source = "df.repartition(1)  # noqa: SDK001, SDK006"
    results = lint(source)
    assert results == []


def test_noqa_case_insensitive():
    source = "df.repartition(200)  # NOQA: sdk001"
    results = lint(source)
    assert results == []


def test_no_noqa_still_flags():
    source = "df.repartition(200)"
    results = lint(source)
    assert len(results) >= 1
    assert any(r.rule_id == "SDK001" for r in results)


def test_noqa_on_different_line_does_not_suppress():
    source = """
df.repartition(200)
x = 1  # noqa: SDK001
""".strip()
    results = lint(source)
    assert any(r.rule_id == "SDK001" for r in results)
