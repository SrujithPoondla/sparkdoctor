# Changelog

## v0.1.8

- **10 new rules:** SDK008 (cross-DataFrame column reference), SDK009 (long chain),
  SDK011 (magic literal in filter/when), SDK018 (inconsistent column refs),
  SDK020 (DROP TABLE before overwrite), SDK024 (streaming without schema),
  SDK028 (distinct().count() two-pass anti-pattern),
  SDK029 (write without explicit mode), SDK030 (redundant sorts in chain)
- **Monorepo restructure:** `packages/python/` layout with per-rule YAML metadata in `core/rules/`
- **Corpus testing:** inline `# expect: SDKXXX` annotations with coverage enforcement
- **Concise output:** terminal output is concise by default, `--verbose` for details
- **Ruff integration:** linter + formatter in CI
- **SDK029:** DataFrameWriter alias tracking — detects `.mode()` omission
  even when the writer is assigned to a variable
- **SDK028:** Dynamic message for distinct() vs dropDuplicates() variants;
  NULL semantics caveat in suggestion
- 28 rules, 298 tests

## v0.1.7

- Updated project description, broadened to Spark
- 19 rules, 207 tests

## v0.1.6

- **Import-based detection:** replaced all name-guessing heuristics with import analysis
  - PySpark import gating: SDK002, SDK007, SDK012 skip files without `import pyspark`
  - Viz import tracking: SDK023 detects matplotlib/plotly/tkinter imports
  - AST-based set tracking: SDK025 tracks variables assigned from set expressions
  - Eliminated 14 false positives across real-world repos (663 → 649 findings)
- **DRY:** shared `_has_pyspark_import()` in `_helpers.py`
- **SDK005 fix:** `useArrow=True` no longer flagged as plain UDF
- 19 rules, 207 tests

## v0.1.5

- **Plugin architecture:** external rules, output formats, and language parsers
  via Python entry points (`sparkdoctor.rules`, `sparkdoctor.outputs`, `sparkdoctor.parsers`)
- **Rule categories:** every rule now has a `category` — `performance`, `correctness`, or `style`
- **`pyproject.toml` config:** `[tool.sparkdoctor]` section for `disable`, `exclude`,
  and `severity_overrides` — no CLI flags needed
- **Language-aware engine:** parser abstraction and per-language rule filtering
  prepare the architecture for future Scala/Java support
- **`--no-config` flag:** ignore `pyproject.toml` configuration
- 19 rules, 166 tests

## v0.1.4

- **DRY refactor:** extracted shared helpers for collect/toPandas-without-limit,
  repartition/coalesce detection, and config-set detection into `_helpers.py`
- **SDK019 fix:** now only flags `inferSchema=True` on Spark read calls
  (`.csv()`, `.json()`, `.load()`), not arbitrary functions
- **SDK023 simplification:** removed overly broad if-block guard that caused
  false negatives; use `# noqa: SDK023` for intentional suppression
- **Severity ordering:** extracted duplicated order dict to module constant
- **Runner cleanup:** replaced `print()` with `logging.warning()`
- **Test coverage:** added runner, output, --exclude, SDK007 edge case, and
  SDK019 false-positive tests
- **SDK005 assertions:** tightened from `>= 1` to exact counts
- 19 rules, 149 tests

## v0.1.3

- **6 new rules:** SDK014 (AQE disabled), SDK015 (hardcoded shuffle partitions),
  SDK017 (select star), SDK019 (inferSchema=True), SDK027 (orderBy before write),
  SDK031 (collect/toPandas in loop)
- **SDK014 fix:** clean boolean vs string comparison for `false` values
- **Rule base class validation:** `__init_subclass__` checks for required attributes
- **Registry uniqueness check:** duplicate rule IDs raise an error at startup
- Validated against 8 real-world repos (757 files, 0 false positives)
- 19 total rules, 128 tests

## v0.1.2

- **`--disable` flag:** suppress specific rules from CLI
  (`sparkdoctor lint . --disable SDK023`)
- **`# noqa` inline suppression:** flake8-style comments to suppress findings per-line
  (`df.show()  # noqa: SDK023`)

## v0.1.1

- **6 new rules:** SDK012 (toPandas without limit), SDK013 (RDD API on DataFrame),
  SDK016 (crossJoin), SDK023 (show in production), SDK025 (union by position),
  SDK026 (f-string SQL injection)
- **`--exclude` flag:** skip files/directories by glob pattern
  (`sparkdoctor lint . --exclude tests`)

## v0.1.0

- Initial release: 7 rules (SDK001-SDK007)
- CLI with `--format json`, `--severity`, `--exit-code`, `--no-color`
- Rich terminal output, JSON canonical output
- Auto-discovery rule registry
