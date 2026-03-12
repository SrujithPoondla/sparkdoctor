"""Tests for SDK023 — show() left in production code."""
import ast

from sparkdoctor.rules.sdk023_show_in_production import ShowInProductionRule

RULE = ShowInProductionRule()


def check(source: str):
    tree = ast.parse(source)
    return RULE.check(tree, source.splitlines())


# ── True positive ───────────────────────────────────────────────────────────


def test_detects_bare_show():
    source = "df.show()"
    results = check(source)
    assert len(results) == 1
    assert results[0].rule_id == "SDK023"


def test_detects_show_with_args():
    source = "df.show(20, truncate=False)"
    results = check(source)
    assert len(results) == 1


def test_detects_show_inside_if():
    """show() inside any if block is still flagged — use # noqa to suppress."""
    source = """
if data_is_valid:
    df.show(5)
""".strip()
    results = check(source)
    assert len(results) == 1


# ── True negative ───────────────────────────────────────────────────────────


def test_allows_non_df_show():
    """A regular function called 'show' should not trigger if not a method call."""
    source = "result = compute_result()"
    results = check(source)
    assert results == []


# ── Edge case ───────────────────────────────────────────────────────────────


def test_detects_chained_show():
    source = 'df.filter(F.col("status") == "active").show(5)'
    results = check(source)
    assert len(results) == 1


def test_detects_multiple_shows():
    source = """
df1.show()
df2.show(10)
""".strip()
    results = check(source)
    assert len(results) == 2


# ── False positive regression: import-based detection ──────────────────────


def test_allows_matplotlib_show():
    """plt.show() is matplotlib, not Spark — detected via import."""
    source = """
import matplotlib.pyplot as plt
plt.show()
""".strip()
    results = check(source)
    assert results == []


def test_allows_figure_show():
    """fig.show() should not fire when fig is assigned from matplotlib."""
    source = """
import matplotlib.pyplot as plt
fig = plt.figure()
fig.show()
""".strip()
    results = check(source)
    assert results == []


def test_allows_subplots_show():
    """fig, ax = plt.subplots() — fig.show() via import tracking."""
    source = """
import matplotlib.pyplot as plt
fig, ax = plt.subplots()
fig.show()
""".strip()
    results = check(source)
    assert results == []


def test_allows_viz_chain_show():
    """ax.imshow(...).show() — visualization chain method, not Spark."""
    source = 'ax.imshow(data).show()'
    results = check(source)
    assert results == []


def test_allows_plotly_show():
    """Plotly figure.show() should not fire."""
    source = """
import plotly.express as px
fig = px.scatter(data, x="x", y="y")
fig.show()
""".strip()
    results = check(source)
    assert results == []


def test_allows_tkinter_show():
    """Tkinter widget.show() should not fire."""
    source = """
import tkinter as tk
root = tk.Tk()
root.show()
""".strip()
    results = check(source)
    assert results == []


def test_still_detects_df_show():
    """df.show() should still fire in files without viz imports."""
    source = "df.show()"
    results = check(source)
    assert len(results) == 1


def test_still_detects_df_show_with_viz_imports():
    """df.show() should still fire even if file has viz imports (df not from viz)."""
    source = """
import matplotlib.pyplot as plt
fig = plt.figure()
df.show()
""".strip()
    results = check(source)
    assert len(results) == 1
