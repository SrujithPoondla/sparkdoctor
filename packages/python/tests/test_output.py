"""Tests for output renderers."""

import json

from rich.console import Console

from sparkdoctor.lint.base import Diagnostic, Severity
from sparkdoctor.output.json_output import render as json_render
from sparkdoctor.output.terminal import render as terminal_render

_SAMPLE = Diagnostic(
    rule_id="SDK001",
    severity=Severity.WARNING,
    message="Hardcoded repartition count: 200",
    explanation="Partition counts become wrong as data grows.",
    suggestion="Let AQE manage partitioning.",
    line=10,
    col=0,
    filename="job.py",
)


# ── JSON output ──────────────────────────────────────────────────────────────


def test_json_render_valid():
    output = json_render([_SAMPLE])
    data = json.loads(output)
    assert isinstance(data, list)
    assert len(data) == 1


def test_json_render_schema():
    output = json_render([_SAMPLE])
    data = json.loads(output)
    d = data[0]
    assert d["rule_id"] == "SDK001"
    assert d["severity"] == "warning"
    assert d["line"] == 10
    assert d["filename"] == "job.py"


def test_json_render_empty():
    output = json_render([])
    assert json.loads(output) == []


# ── Terminal output ──────────────────────────────────────────────────────────


def test_terminal_render_contains_rule_id():
    console = Console(file=None, no_color=True, width=120)
    with console.capture() as capture:
        terminal_render([_SAMPLE], file_count=1, console=console)
    output = capture.get()
    assert "SDK001" in output
    assert "job.py" in output


def test_terminal_render_empty():
    console = Console(file=None, no_color=True, width=120)
    with console.capture() as capture:
        terminal_render([], file_count=0, console=console)
    output = capture.get()
    assert "0 findings" in output
