# Writing a New SparkDoctor Rule

This guide walks you through adding a rule from scratch. The full process takes
about 30 minutes for a well-understood anti-pattern. You do not need to understand
the engine, CLI, or any code outside `sparkdoctor/rules/` and `tests/rules/`.

---

## Before You Start

Check the existing rule list in the README and `docs/rules/RULE_SPECS.md`.

Ask yourself:
1. **Is this a real problem?** Have you seen it in production code or multiple Stack Overflow questions?
2. **Is detection reliable?** Can you write a check that does not fire on legitimate code?
3. **Is the fix actionable?** Can you write a concrete suggestion with a code example?

If yes to all three, open an issue on GitHub describing the rule before implementing it.
This avoids duplicate work and gets early feedback.

---

## Step 1: Choose a Rule ID

Rule IDs are sequential: `SDK001`, `SDK002`, ... `SDK099`.

Check the existing rules to find the next available number. Add it to the issue
you opened before starting.

---

## Step 2: Create the Rule File

Create `sparkdoctor/rules/sdk0NN_your_rule_name.py`. Use underscores and lowercase.

Copy this template exactly:

```python
"""
SDK0NN — Short title of what this rule detects.

Severity: WARNING | ERROR | INFO
"""
from __future__ import annotations

import ast
from typing import List

from sparkdoctor.lint.base import Diagnostic, Rule, Severity


class YourRuleName(Rule):
    """One-line description of what this rule detects."""

    rule_id  = "SDK0NN"
    severity = Severity.WARNING   # change to ERROR or INFO as appropriate
    title    = "Short title shown in terminal output"

    _EXPLANATION = (
        "Why this pattern is a problem. 2-3 sentences. "
        "Be specific about the performance impact. "
        "No jargon — assume the reader is a competent developer who hasn't studied Spark internals."
    )

    _SUGGESTION = (
        "Concrete fix. Include a code example. "
        "Start with the easiest fix. "
        "Mention any Spark version requirements if relevant."
    )

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        diagnostics = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            # Your detection logic here.
            # Use the AST patterns in docs/rules/RULE_SPECS.md as building blocks.

            if self._is_the_pattern(node):
                diagnostics.append(
                    Diagnostic(
                        rule_id=self.rule_id,
                        severity=self.severity,
                        message=f"Short message with specific detail: {detail}",
                        explanation=self._EXPLANATION,
                        suggestion=self._SUGGESTION,
                        line=node.lineno,
                        col=node.col_offset,
                    )
                )

        return diagnostics

    def _is_the_pattern(self, node: ast.Call) -> bool:
        """Return True if this Call node matches the anti-pattern."""
        # Your detection logic, extracted into a helper for testability.
        ...
```

---

## Step 3: Write Three Tests

Create `tests/rules/test_sdk0NN.py`:

```python
"""Tests for SDK0NN — Your Rule Title."""
import ast
import pytest

from sparkdoctor.rules.sdk0NN_your_rule_name import YourRuleName


RULE = YourRuleName()


def check(source: str):
    """Parse source and run the rule. Returns list of Diagnostics."""
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────

def test_detects_the_pattern():
    """The anti-pattern is present — should return exactly one diagnostic."""
    source = """
df.the_bad_call(200)
""".strip()
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK0NN"
    assert results[0].line == 1


# ── True negative ───────────────────────────────────────────────────────────

def test_does_not_flag_correct_usage():
    """The correct pattern is used — should return no diagnostics."""
    source = """
df.the_correct_call(variable_name)
""".strip()
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────

def test_edge_case_description():
    """Describe the edge case being tested."""
    source = """
# Code representing the edge case
""".strip()
    results = check(source)
    # Assert the expected behavior for this edge case
    assert len(results) == 0   # or 1, depending on expected behavior
```

Run them: `pytest tests/rules/test_sdk0NN.py -v`

All three must pass before submitting.

---

## Step 4: Create a Rule Documentation Page

Create `docs/rules/SDK0NN.md`:

```markdown
# SDK0NN — Your Rule Title

**Severity:** WARNING  
**Added in:** v0.2.0

## Problem

Describe what the anti-pattern is and why it matters. 2-4 sentences.
Write for a developer who has used PySpark but does not know this specific gotcha.

## Example (Bad)

```python
# This triggers SDK0NN
df.the_bad_call(200)
df.another_form_of_bad(literal_value)
```

## Example (Good)

```python
# This does not trigger SDK0NN
df.the_correct_call(variable)
df.better_approach()
```

## Why This Matters

Explain the performance impact with specifics.
If you can quantify it ("10-100x slower"), do so.
Mention which Spark versions or workload types are most affected.

## Exceptions

Note any cases where the pattern is actually acceptable,
and why the rule doesn't fire (or fires but can be suppressed).

## References

- Link to Spark documentation
- Link to relevant benchmark or blog post
- Link to the Stack Overflow question this came from (if applicable)
```

---

## Step 5: Add Corpus Annotations

Add your rule to the corpus test files in `tests/corpus/`. Each rule needs:
- At least one **positive** annotation: a line with `# expect: SDK0NN` where the rule should fire
- At least one **negative** annotation: a line with `# expect: none` where it should not

You can add annotations to existing corpus files or create a new one:

```python
# your_pattern.py — SDK0NN
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# Bad pattern
df.the_bad_call(200)  # expect: SDK0NN

# Good pattern
df.the_correct_call(variable)  # expect: none
```

---

## Step 6: Verify Everything Passes

Run the full CI pipeline locally before pushing:

```bash
make ci      # mirrors the GitHub Actions CI workflow exactly
make build   # mirrors the PyPI publish workflow
```

This runs ruff check, ruff format, pytest, self-lint, and rule validation — the same
steps that run in CI. If `make ci` passes locally, the GitHub workflow will pass too.

You can also run individual steps:

```bash
make test          # pytest only
make lint          # ruff check only
make format-check  # ruff format check only
make self-lint     # sparkdoctor lint on its own source
```

All existing tests must still pass. Your new tests must pass.
No existing test should change behavior because of your rule.

---

## Step 7: Open a Pull Request

PR title format: `feat(rules): Add SDK0NN — Your Rule Title`

PR description must include:
- Link to the GitHub issue
- One real-world example of this anti-pattern from open-source code (with link)
- Confirmation that `make ci` passes locally
- Confirmation that `sparkdoctor lint tests/fixtures/clean_job.py` produces zero findings

---

## Rule Quality Checklist

Before submitting, verify:

- [ ] Rule ID is the next sequential number
- [ ] Rule fires on `tests/fixtures/bad_job.py` (add the pattern to that file)
- [ ] Rule does NOT fire on `tests/fixtures/clean_job.py`
- [ ] Corpus annotations added (positive + negative)
- [ ] `make ci` passes — all tests, ruff, self-lint green
- [ ] `make build` passes — wheel and sdist build successfully
- [ ] `explanation` is 2-3 sentences, no jargon, explains the impact
- [ ] `suggestion` includes a concrete code example
- [ ] `severity` is justified in the PR description
- [ ] Zero false positives on the top 10 GitHub PySpark repos (manual spot check)

---

## Common AST Patterns

See `docs/rules/RULE_SPECS.md` for a reference of common AST patterns used in
existing rules. Use these as building blocks rather than writing from scratch.

The most useful patterns are documented there:
- Detecting method calls by name
- Checking for literal integer arguments
- Walking inside loop bodies only
- Tracking variable names across a file
