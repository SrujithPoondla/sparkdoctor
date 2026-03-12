"""
SDK018 — Inconsistent column reference style.

Severity: INFO
"""

from __future__ import annotations

import ast

from sparkdoctor.lint.base import Diagnostic, Rule
from sparkdoctor.rules._helpers import _has_pyspark_import


class InconsistentColumnRefRule(Rule):
    """Detects mixing of F.col(), df["col"], and df.col styles in the same file."""

    rule_id = "SDK018"

    # DataFrame methods that return DataFrames — used to identify DF variables
    # and attribute access context. Any method here is also excluded from
    # column-access detection via _NON_COLUMN_ATTRS.
    _DF_METHODS = {
        "select",
        "filter",
        "where",
        "withColumn",
        "groupBy",
        "orderBy",
        "sort",
        "agg",
        "drop",
        "join",
        "union",
        "unionAll",
        "crossJoin",
        "limit",
        "distinct",
        "dropDuplicates",
        "withColumnRenamed",
        "alias",
        "toDF",
        "unionByName",
        "sortWithinPartitions",
        "repartition",
        "coalesce",
        "sample",
        "sampleBy",
        "subtract",
        "intersect",
        "exceptAll",
        "transform",
        "checkpoint",
        "localCheckpoint",
        "mapInPandas",
        "mapInArrow",
        "randomSplit",
        "withWatermark",
    }

    # Attributes that are NOT column access on a DataFrame.
    # Methods already in _DF_METHODS are included via the union at the end.
    _NON_COLUMN_ATTRS = {
        "columns",
        "schema",
        "dtypes",
        "rdd",
        "write",
        "writeStream",
        "read",
        "readStream",
        "sql",
        "conf",
        "sparkContext",
        "udf",
        "cache",
        "persist",
        "unpersist",
        "count",
        "collect",
        "show",
        "head",
        "first",
        "take",
        "toPandas",
        "asc",
        "desc",
        "cast",
        "otherwise",
        "over",
        "between",
        "isin",
        "isNull",
        "isNotNull",
        "na",
        "stat",
        "explain",
        "printSchema",
        "describe",
        "summary",
        "createOrReplaceTempView",
        "createTempView",
        "isEmpty",
        "observe",
        "foreach",
        "foreachPartition",
        "toJSON",
        "toLocalIterator",
        "freqItems",
        "approxQuantile",
        "crosstab",
        "cov",
        "corr",
        "isStreaming",
        "isLocal",
    } | _DF_METHODS

    def check(self, tree: ast.AST, source_lines: list[str]) -> list[Diagnostic]:
        if not _has_pyspark_import(tree):
            return []

        # Step 1: Resolve functions module aliases from imports.
        func_aliases = self._find_functions_aliases(tree)
        has_bare_col = self._has_bare_col_import(tree)

        # Step 2: Find DataFrame variable names (assigned from DF method chains).
        df_vars = self._find_df_variables(tree)

        # Step 3: Scan entire file for column reference styles.
        styles: dict[str, list[int]] = {}

        for node in ast.walk(tree):
            # F.col() / functions.col() / sf.col() / bare col()
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute) and node.func.attr == "col":
                    receiver = node.func.value
                    if isinstance(receiver, ast.Name) and receiver.id in func_aliases:
                        styles.setdefault("F.col()", []).append(node.lineno)
                elif has_bare_col and isinstance(node.func, ast.Name) and node.func.id == "col":
                    styles.setdefault("F.col()", []).append(node.lineno)

            # df["col"] — only on known DataFrame variables
            if (
                isinstance(node, ast.Subscript)
                and isinstance(node.value, ast.Name)
                and node.value.id in df_vars
                and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)
            ):
                styles.setdefault('df["col"]', []).append(node.lineno)

            # df.col — attribute access on known DataFrame variables
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id in df_vars
                and node.attr not in self._NON_COLUMN_ATTRS
            ):
                # Exclude if this attribute is itself a call target (df.select())
                # We can't detect parent in ast.walk, so we rely on the exclusion set
                styles.setdefault("df.col", []).append(node.lineno)

        if len(styles) < 2:
            return []

        style_names = sorted(styles.keys())
        min_style = min(style_names, key=lambda s: len(styles[s]))
        first_line = min(styles[min_style])

        return [
            Diagnostic(
                rule_id=self.rule_id,
                severity=self.severity,
                message=f"Mixed column reference styles: {', '.join(style_names)}",
                explanation=self._EXPLANATION,
                suggestion=self._SUGGESTION,
                line=first_line,
                col=0,
            )
        ]

    def _find_functions_aliases(self, tree: ast.AST) -> set[str]:
        """Find all aliases for pyspark.sql.functions (e.g. F, sf, functions)."""
        aliases: set[str] = set()
        for node in ast.walk(tree):
            # import pyspark.sql.functions as F
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "pyspark.sql.functions":
                        aliases.add(alias.asname or "functions")
            # from pyspark.sql import functions as F
            elif isinstance(node, ast.ImportFrom) and node.module == "pyspark.sql":
                for alias in node.names:
                    if alias.name == "functions":
                        aliases.add(alias.asname or "functions")
        return aliases

    def _has_bare_col_import(self, tree: ast.AST) -> bool:
        """Check if `col` is directly imported (from pyspark.sql.functions import col)."""
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module
                and node.module in ("pyspark.sql.functions", "pyspark.sql")
            ):
                for alias in node.names:
                    if alias.name == "col":
                        return True
        return False

    def _find_df_variables(self, tree: ast.AST) -> set[str]:
        """Find variable names that are likely DataFrames.

        Tracks names assigned from:
        - spark.read / spark.readStream chains
        - DataFrame method calls (.filter(), .select(), etc.)
        - spark.createDataFrame() / spark.table() / spark.sql()
        """
        df_vars: set[str] = set()
        # Common conventional names
        df_vars.add("df")

        spark_creators = {"read", "readStream", "createDataFrame", "table", "sql", "range"}

        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not isinstance(node.value, ast.Call):
                continue

            # Check if RHS is a DF method chain
            call = node.value
            if isinstance(call.func, ast.Attribute):
                method = call.func.attr
                if method in self._DF_METHODS or method in spark_creators:
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            df_vars.add(target.id)
                    continue

                # Check receiver chain for DF methods (e.g. spark.read.csv())
                if self._chain_has_df_method(call):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            df_vars.add(target.id)

        return df_vars

    def _chain_has_df_method(self, node: ast.AST) -> bool:
        """Check if a call chain contains any DataFrame method."""
        current = node
        spark_creators = {"read", "readStream", "createDataFrame", "table", "sql", "range"}
        all_methods = self._DF_METHODS | spark_creators
        while True:
            if isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
                if current.func.attr in all_methods:
                    return True
                current = current.func.value
            elif isinstance(current, ast.Attribute):
                if current.attr in all_methods:
                    return True
                current = current.value
            else:
                return False
