"""
SDK023 — show() left in production code.

Severity: INFO
"""
from __future__ import annotations

import ast

from sparkdoctor.lint.base import Category, Diagnostic, Rule, Severity
from sparkdoctor.rules._helpers import (
    chain_contains_method,
    chain_root_name,
    is_method_call,
    receiver_name,
)


class ShowInProductionRule(Rule):
    """Detects .show() calls left in production code."""

    rule_id = "SDK023"
    severity = Severity.INFO
    title = "show() left in production code"
    category = Category.STYLE

    _EXPLANATION = (
        "Each .show() call triggers a Spark action — a full job execution that "
        "materializes the DataFrame and pulls rows to the driver. A pipeline with "
        "multiple show() calls left in production runs extra jobs before the actual "
        "work begins, wasting compute and adding latency."
    )

    _SUGGESTION = (
        "Remove .show() calls from production code, or suppress with "
        "# noqa: SDK023 if intentional.\n"
        "For logging, use:\n"
        "  import logging\n"
        "  logger = logging.getLogger(__name__)\n"
        '  logger.info(f"Row count: {df.count()}")'
    )

    # Chain methods that indicate matplotlib / visualization, not Spark
    _VIZ_CHAIN_METHODS = {
        "subplots", "subplot", "imshow", "bar", "scatter", "hist",
        "plot", "pie", "boxplot", "violinplot", "heatmap",
    }

    # Libraries whose .show() is not Spark
    _VIZ_IMPORT_MODULES = {
        "matplotlib", "plotly", "bokeh", "seaborn", "yellowbrick",
        "altair", "holoviews", "pygal",
        "tkinter", "PyQt5", "PyQt6", "PySide2", "PySide6", "wx",
        "kivy", "pygame",
        "IPython",
    }

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        has_viz_imports = self._has_viz_imports(tree)
        viz_vars = self._find_viz_variables(tree) if has_viz_imports else set()
        diagnostics: list[Diagnostic] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not is_method_call(node, "show"):
                continue
            if chain_contains_method(node, self._VIZ_CHAIN_METHODS):
                continue
            # In files with viz imports, skip show() on variables from viz origins
            if has_viz_imports:
                name = receiver_name(node)
                if name and name in viz_vars:
                    continue
                # self.fig.show(), self.widget.show() — attribute access in viz files
                root = chain_root_name(node)
                if root == "self":
                    continue
            diagnostics.append(
                Diagnostic(
                    rule_id=self.rule_id,
                    severity=self.severity,
                    message=".show() triggers a Spark action — "
                    "remove or guard in production code",
                    explanation=self._EXPLANATION,
                    suggestion=self._SUGGESTION,
                    line=node.lineno,
                    col=node.col_offset,
                )
            )
        return diagnostics

    def _has_viz_imports(self, tree: ast.AST) -> bool:
        """Return True if the file imports matplotlib or other viz libraries."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] in self._VIZ_IMPORT_MODULES:
                        return True
            elif isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] in self._VIZ_IMPORT_MODULES:
                return True
        return False

    def _find_viz_variables(self, tree: ast.AST) -> set[str]:
        """Find variables assigned from viz library calls or import aliases."""
        viz_vars: set[str] = set()
        # Collect import aliases: import matplotlib.pyplot as plt -> plt
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] in self._VIZ_IMPORT_MODULES:
                        name = alias.asname or alias.name.split(".")[-1]
                        viz_vars.add(name)
            elif isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] in self._VIZ_IMPORT_MODULES:
                for alias in node.names:
                        name = alias.asname or alias.name
                        viz_vars.add(name)
        # Track variables assigned from viz calls (fig = plt.figure())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not isinstance(node.value, ast.Call):
                continue
            root = chain_root_name(node.value)
            if root and root in viz_vars:
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        viz_vars.add(target.id)
                    elif isinstance(target, ast.Tuple):
                        for elt in target.elts:
                            if isinstance(elt, ast.Name):
                                viz_vars.add(elt.id)
        return viz_vars
