"""Tests for the plugin/extensibility system."""

import ast

from sparkdoctor.lint.base import (
    Category,
    Diagnostic,
    PythonParser,
    Rule,
    Severity,
)
from sparkdoctor.lint.engine import LintEngine
from sparkdoctor.rules.registry import get_all_parsers, get_all_rules

# ── Category on rules ──────────────────────────────────────────────────────


def test_all_rules_have_category():
    """Every rule must declare a category."""
    rules = get_all_rules()
    for rule in rules:
        assert isinstance(rule.category, Category), f"{rule.rule_id} has no valid category"


def test_category_values():
    """Category enum has the three expected values."""
    assert set(Category) == {
        Category.PERFORMANCE,
        Category.CORRECTNESS,
        Category.STYLE,
    }


# ── Language on rules ──────────────────────────────────────────────────────


def test_all_rules_have_language():
    """Every built-in rule should be 'python'."""
    rules = get_all_rules()
    for rule in rules:
        assert rule.language == "python", f"{rule.rule_id} has unexpected language: {rule.language}"


# ── api_version ────────────────────────────────────────────────────────────


def test_all_rules_have_api_version():
    rules = get_all_rules()
    for rule in rules:
        assert rule.api_version == 1, (
            f"{rule.rule_id} has unexpected api_version: {rule.api_version}"
        )


# ── Parser registry ───────────────────────────────────────────────────────


def test_python_parser_registered():
    parsers = get_all_parsers()
    assert "python" in parsers
    assert isinstance(parsers["python"], PythonParser)


def test_python_parser_extensions():
    parser = PythonParser()
    assert parser.file_extensions == (".py",)


def test_python_parser_parses():
    parser = PythonParser()
    tree = parser.parse("x = 1", filename="test.py")
    assert isinstance(tree, ast.AST)


# ── Engine language filtering ──────────────────────────────────────────────


class _FakeScalaRule(Rule):
    rule_id = "FAKE001"
    severity = Severity.WARNING
    title = "Fake Scala rule"
    category = Category.PERFORMANCE
    language = "scala"

    def check(self, tree, source_lines):
        return [
            Diagnostic(
                rule_id=self.rule_id,
                severity=self.severity,
                message="found something",
                explanation="test",
                suggestion="test",
                line=1,
                col=0,
            )
        ]


def test_engine_filters_by_language():
    """Engine should only run rules matching the requested language."""
    engine = LintEngine(rules=[_FakeScalaRule()])
    tree = ast.parse("x = 1")
    # Running as 'python' should skip the scala rule
    results = engine.check(tree, ["x = 1"], language="python")
    assert results == []
    # Running as 'scala' should include it
    results = engine.check(tree, ["x = 1"], language="scala")
    assert len(results) == 1


# ── Rule validation ───────────────────────────────────────────────────────


def test_rule_subclass_validation():
    """Rule subclass missing required attrs should raise TypeError."""
    try:

        class BadRule(Rule):
            rule_id = "BAD001"
            severity = Severity.ERROR
            # missing 'title'

            def check(self, tree, source_lines):
                return []

        raise AssertionError("Should have raised TypeError")
    except TypeError as exc:
        assert "title" in str(exc)
