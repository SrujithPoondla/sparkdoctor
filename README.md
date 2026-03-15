# SparkDoctor

**A Spark linter — catches performance, correctness, and style issues before they reach your cluster.**

```
pip install sparkdoctor
sparkdoctor lint path/to/job.py
```

No Spark installation required. No cluster. No configuration.

---

## What it does

SparkDoctor parses your Spark code and flags performance anti-patterns,
correctness bugs, and style issues — before you run anything.

```
$ sparkdoctor lint jobs/pipeline.py

╭───────────────────────  SparkDoctor v0.1.8  ───────────────────────╮
│ Scanning: jobs/pipeline.py                                         │
╰────────────────────────────────────────────────────────────────────╯

  jobs/pipeline.py

  ⚠  line 34  SDK001  Hardcoded repartition count: 200
  ✖  line 67  SDK004  withColumn() called inside a loop
  ✖  line 89  SDK006  repartition(1) forces all data through a single task
  ⚠  line 112 SDK029  DataFrame write without explicit .mode()
────────────────────────────────────────────────────────────────────────
  4 findings  (2 errors, 2 warnings)  in 1 file
────────────────────────────────────────────────────────────────────────
```

Use `--verbose` for explanations and fix suggestions.

---

## Rules

### Performance (v0.1.0)

| ID | Title | Category | Severity |
|----|-------|----------|----------|
| SDK001 | Hardcoded repartition or coalesce count | performance | ⚠ warning |
| SDK002 | collect() without a preceding limit() | performance | ⚠ warning |
| SDK003 | count() used as an emptiness check | style | ⚠ warning |
| SDK004 | withColumn() called inside a loop | performance | ✖ error |
| SDK005 | Python UDF without Arrow optimization | performance | ⚠ warning |
| SDK006 | repartition(1) or coalesce(1) | performance | ✖ error |
| SDK007 | cache() / persist() without unpersist() | performance | ℹ info |

### Performance & Safety (v0.1.1)

| ID | Title | Category | Severity |
|----|-------|----------|----------|
| SDK012 | toPandas() without a preceding limit() | performance | ✖ error |
| SDK013 | RDD API used on DataFrame | performance | ✖ error |
| SDK016 | crossJoin() produces a cartesian product | performance | ⚠ warning |
| SDK023 | show() left in production code | style | ℹ info |
| SDK025 | union() matches columns by position, not name | correctness | ⚠ warning |
| SDK026 | Dynamic string in spark.sql() — SQL injection risk | correctness | ⚠ warning |

### Configuration & Optimization (v0.1.3)

| ID | Title | Category | Severity |
|----|-------|----------|----------|
| SDK014 | AQE explicitly disabled | performance | ⚠ warning |
| SDK015 | Hardcoded spark.sql.shuffle.partitions | performance | ⚠ warning |
| SDK017 | select("*") reads all columns | style | ⚠ warning |
| SDK019 | inferSchema=True in production read | correctness | ⚠ warning |
| SDK027 | orderBy()/sort() before write is wasteful | performance | ⚠ warning |
| SDK031 | collect() or toPandas() inside a loop | performance | ✖ error |

### Code Quality (v0.1.7)

| ID | Title | Category | Severity |
|----|-------|----------|----------|
| SDK008 | Cross-DataFrame column reference | correctness | ⚠ warning |
| SDK009 | Long transformation chain without intermediate assignment | style | ℹ info |
| SDK011 | Magic literal in filter/when condition | style | ⚠ warning |
| SDK018 | Inconsistent column reference style | style | ℹ info |
| SDK020 | DROP TABLE or fs.rm before overwrite write | correctness | ℹ info |
| SDK024 | Streaming read without explicit schema | correctness | ℹ info |

### Anti-Patterns (v0.1.8)

| ID | Title | Category | Severity |
|----|-------|----------|----------|
| SDK028 | distinct().count() is a two-pass operation | performance | ⚠ warning |
| SDK029 | DataFrame write without explicit mode | correctness | ⚠ warning |
| SDK030 | Multiple orderBy()/sort() — earlier sorts are wasted | performance | ⚠ warning |

Full rule documentation in [`docs/rules/`](docs/rules/).

---

## Validated Against Real-World Repos

SparkDoctor was tested against 8 popular open-source PySpark repositories
with **zero false positives**:

| Repository | Stars | Findings | Notes |
|------------|------:|---------:|-------|
| [pyspark-examples](https://github.com/spark-examples/pyspark-examples) | 1.3K | 508 | Tutorial repo — `.show()` calls are intentional (SDK023) |
| [optimus](https://github.com/hi-primus/optimus) | 1.5K | 34 | Framework wrapping Spark — zero FPs in Polars engine |
| [petastorm](https://github.com/uber/petastorm) | 1.9K | 15 | Uber's data library — legitimate findings in tests/examples |
| [pyspark-ai](https://github.com/pyspark-ai/pyspark-ai) | 876 | 27 | f-string SQL in test cleanup code (SDK026) |
| [sparkit-learn](https://github.com/lensacom/sparkit-learn) | 1.2K | 59 | ML library — `.collect()` in test assertions |
| [pyspark-example-project](https://github.com/AlexIoannides/pyspark-example-project) | 2.1K | 5 | `coalesce(1)` for single-file output (SDK006) |
| [sagemaker-pyspark-processing](https://github.com/aws-samples/sagemaker-pyspark-processing) | ~100 | 1 | `coalesce(10)` hardcoded (SDK001) |
| [sparkmagic](https://github.com/jupyter-incubator/sparkmagic) | 1.4K | 0 | Jupyter kernel — no PySpark anti-patterns |

**649 total findings — every finding inspected, zero false positives.**

Import-based detection ensures non-Spark libraries (Polars, Dask, TensorFlow, matplotlib)
are never flagged, even in mixed-import files.

Use `--exclude tests` and `--disable SDK023` to reduce noise on tutorial/test codebases.

---

## Install

Requires Python 3.10+. No Spark installation needed.

```bash
pip install sparkdoctor
```

---

## Usage

```bash
# Lint a single file
sparkdoctor lint jobs/pipeline.py

# Lint a directory (recursive)
sparkdoctor lint jobs/

# JSON output for CI integration
sparkdoctor lint jobs/ --format json

# Only show errors, not warnings
sparkdoctor lint jobs/ --severity error

# Exit with code 1 if findings exist (for CI blocking)
sparkdoctor lint jobs/ --exit-code

# Exclude test directories
sparkdoctor lint jobs/ --exclude tests --exclude fixtures

# Disable specific rules
sparkdoctor lint jobs/ --disable SDK023 --disable SDK025

# Combine: fail CI on any error-level finding
sparkdoctor lint jobs/ --severity error --exit-code
```

### Project Configuration

Configure SparkDoctor in your `pyproject.toml` — no CLI flags needed:

```toml
[tool.sparkdoctor]
disable = ["SDK023", "SDK025"]
exclude = ["tests", "fixtures"]
```

CLI flags override `pyproject.toml` settings. Use `--no-config` to ignore the file entirely.

### Inline Suppression

Suppress findings on a specific line with `# noqa` comments:

```python
df.repartition(200)  # noqa: SDK001          — suppress one rule
df.repartition(1)    # noqa: SDK001, SDK006  — suppress multiple rules
df.show()            # noqa                  — suppress all rules on this line
```

---

## CI Integration

### GitHub Actions

```yaml
- name: SparkDoctor lint
  run: |
    pip install sparkdoctor
    sparkdoctor lint spark_jobs/ --exit-code
```

### pre-commit

```yaml
repos:
  - repo: local
    hooks:
      - id: sparkdoctor
        name: SparkDoctor PySpark linter
        entry: sparkdoctor lint
        language: python
        types: [python]
        pass_filenames: true
```

---

## Plugin Development

SparkDoctor supports external plugins via Python entry points. You can add custom
rules, output formats, or language parsers without forking.

### Custom Rules

```python
# In your package: my_spark_rules/rules.py
from sparkdoctor.lint.base import Rule, Diagnostic, Severity, Category

class MyCustomRule(Rule):
    rule_id = "CUSTOM001"
    severity = Severity.WARNING
    title = "My custom check"
    category = Category.PERFORMANCE

    def check(self, tree, source_lines):
        # Your AST analysis here
        return []
```

Register via entry points in your `pyproject.toml`:

```toml
[project.entry-points."sparkdoctor.rules"]
my_rules = "my_spark_rules.rules"
```

Install your package and SparkDoctor picks up the rules automatically.

### Custom Output Formats

Register an output renderer under `sparkdoctor.outputs`:

```toml
[project.entry-points."sparkdoctor.outputs"]
sarif = "my_package.sarif_output:render"
```

Then use it: `sparkdoctor lint jobs/ --format sarif`

### Language Parsers

SparkDoctor's architecture supports language plugins. A parser plugin provides
a `Parser` subclass and language-specific rules:

```toml
[project.entry-points."sparkdoctor.parsers"]
scala = "sparkdoctor_scala.parser:ScalaParser"
```

---

## Development

```bash
# Set up
python3 -m venv .venv
source .venv/bin/activate
pip install -e "packages/python[dev]"

# Run the full CI pipeline locally (mirrors GitHub Actions exactly)
make ci

# Build wheel + sdist (mirrors the PyPI publish workflow)
make build

# Individual steps
make test          # pytest
make lint          # ruff check
make format        # ruff format (auto-fix)
make format-check  # ruff format (check only)
make self-lint     # sparkdoctor lint on its own source
make clean         # remove build artifacts
```

Always run `make ci` before pushing. If it passes locally, CI will pass on GitHub.

---

## Contributing

The best contribution is a new rule. If you have seen an anti-pattern burn your team,
add it so it can't burn the next team.

See [Contributing a Rule](docs/contributing/writing-a-rule.md) — the full process
takes about 30 minutes.

```
sparkdoctor/rules/sdk0NN_your_rule.py    # one file
tests/rules/test_sdk0NN.py               # one test file
docs/rules/SDK0NN.md                     # one doc page
```

No engine knowledge needed. Check the issues tab for rule candidates.

---

## Roadmap

**v0.1.x** — Static linter: rules, CLI, inline suppression (current)

**v0.2.0** — Scala support via [scalameta](https://scalameta.org/) + sbt plugin

**v0.3.0** — Java support via [JavaParser](https://javaparser.org/) + Maven/Gradle plugins

**v0.4.0** — Metadata profiler: read Delta logs, Parquet footers, and Iceberg manifests
to generate config recommendations without running a job.

**v0.5.0** — Config advisor: given data profile + target platform, output specific
configuration for EMR, Glue, Dataproc, and Databricks.

**v0.6.0** — Migration advisor: detect jobs that don't need Spark and suggest
DuckDB or Polars equivalents.

---

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for the full release history.

---

## License

Apache 2.0
