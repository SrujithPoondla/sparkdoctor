# SparkDoctor — Rule Specifications v0.1

This document contains the full specification for each rule: what to detect, what NOT to
detect, how to implement the AST check, and what the output should say.

---

## SDK001 — Hardcoded Repartition Count

**Severity:** WARNING
**Title:** Hardcoded repartition count

### What to detect

`repartition(N)` or `coalesce(N)` where `N` is a literal integer.

```python
# DETECT these:
df.repartition(200)
df.coalesce(50)
df.repartition(200, "country")   # literal count even with column arg
```

### What NOT to detect

```python
# DO NOT detect these:
df.repartition(num_partitions)          # variable
df.repartition(calculate_partitions())  # function call
df.coalesce(1)                          # handled by SDK006 instead
df.repartition(1)                       # handled by SDK006 instead
```

### AST detection approach

Walk all `ast.Call` nodes. Match where:
- `func` is an `ast.Attribute` with `.attr` in `{"repartition", "coalesce"}`
- First argument (`args[0]`) is an `ast.Constant` with `isinstance(value, int)`
- That integer is greater than 1 (SDK006 handles the == 1 case)

### Output text

**message:** `Hardcoded repartition count: {N}`

**explanation:**
```
Partition counts hardcoded at development time become wrong as data grows. A job
tuned for 50 GB creates undersized or oversized partitions when data changes. The
count that was optimal last month may cause spill to disk or underutilization today.
```

**suggestion:**
```
Remove the hardcoded count and let AQE manage partitioning automatically
(requires spark.sql.adaptive.enabled=true, which is the default in Spark 3.2+).
If you need explicit control, derive the count from data size:
  num_partitions = max(df.rdd.getNumPartitions(), estimated_rows // 1_000_000)
```

---

## SDK002 — Collect Without Limit

**Severity:** WARNING
**Title:** collect() without a preceding limit()

### What to detect

`df.collect()` where there is no `.limit(N)` call in the same method chain immediately
before `.collect()`.

```python
# DETECT these:
df.collect()
df.filter(...).collect()
df.join(...).groupBy(...).collect()
```

### What NOT to detect

```python
# DO NOT detect these:
df.limit(100).collect()
df.filter(...).limit(1000).collect()
```

### AST detection approach

Walk all `ast.Call` nodes where the function is an attribute named `collect`.
Check the receiver: if the receiver is itself a `Call` with attribute name `limit`,
do not flag it. Otherwise flag it.

For chained calls like `df.filter(...).limit(100).collect()`, the immediate parent
call's attribute should be `limit`. Walk up the chain one level only.

### Output text

**message:** `collect() called without a preceding limit()`

**explanation:**
```
collect() moves the entire DataFrame to the driver. On a large dataset this causes
driver OOM, long GC pauses, and job failure. There is no automatic protection —
Spark will attempt to transfer all rows regardless of size.
```

**suggestion:**
```
Add .limit(N) before .collect() to bound the result:
  df.limit(10_000).collect()
If you genuinely need all rows on the driver, use .toPandas() on a sampled subset,
or write to storage and read locally.
```

---

## SDK003 — Count as Emptiness Check

**Severity:** WARNING
**Title:** count() used as an emptiness check

### What to detect

Comparisons of the form:
```python
df.count() == 0
df.count() != 0
df.count() > 0
df.count() >= 1
0 == df.count()
```

### What NOT to detect

```python
# DO NOT detect:
row_count = df.count()          # assignment, not comparison
print(df.count())               # print, not comparison
assert df.count() > 100         # This one is debatable — flag it, it's still a full scan
```

Actually — flag `df.count()` in any comparison expression. A full scan for a boolean
answer is always wrong.

### AST detection approach

Walk `ast.Compare` nodes. Check if any comparator or the left operand is a `Call`
where the function attribute is `count`. Also catch `ast.BoolOp` containing such
comparisons.

### Output text

**message:** `count() used to check if DataFrame is empty`

**explanation:**
```
count() forces a full scan of the entire dataset to return a single number. Using
it to answer a yes/no question (is this DataFrame empty?) wastes all that compute.
On a 10 TB table, you scan 10 TB to learn the answer is "no".
```

**suggestion:**
```
Use isEmpty() or limit(1) instead:
  if df.isEmpty():          # short-circuits after finding the first row
      ...
  if df.limit(1).count() == 0:   # scans at most 1 row
      ...
Note: isEmpty() is available in Spark 2.4+.
```

---

## SDK004 — WithColumn in Loop

**Severity:** ERROR
**Title:** withColumn() called inside a loop

### What to detect

Any `df.withColumn(...)` call (or any method chain ending in `.withColumn(...)`)
that appears inside the body of a `for` loop or `while` loop.

```python
# DETECT:
for col in columns:
    df = df.withColumn(col, ...)

while condition:
    df = df.withColumn(name, expr)
```

### What NOT to detect

```python
# DO NOT detect:
df = df.withColumn("new_col", ...)   # outside any loop
```

### AST detection approach

Walk `ast.For` and `ast.While` nodes. For each loop node, walk its `body` (and
`orelse`) sub-tree. In that sub-tree, look for `ast.Call` nodes where the function
is an `ast.Attribute` with `.attr == "withColumn"`. Use a nested visitor or
recursive walk within the loop body.

**Important:** This is nested AST walking. Use a helper that walks only within
the loop body, not the entire tree (to avoid false negatives from non-nested calls).

### Output text

**message:** `withColumn() called {N} times inside a loop`
(N = number of withColumn calls detected in the loop body)

**explanation:**
```
Every withColumn() call creates a new DataFrame and adds one projection to the
query plan. Calling it in a loop with 50 columns creates 50 nested projections.
Spark's query plan analyzer must process all 50, causing O(N²) plan compilation
time. At 100+ columns this can take minutes before the job even starts.
```

**suggestion:**
```
Collect all column transformations into a list and call select() once:
  from pyspark.sql import functions as F
  new_cols = [F.col(c).cast("string").alias(c) for c in columns]
  df = df.select("*", *new_cols)
```

---

## SDK005 — Python UDF Without Arrow

**Severity:** WARNING
**Title:** Python UDF that could use Arrow optimization

### What to detect

- `@udf` decorator on a function definition
- `udf(lambda ...)` calls
- `spark.udf.register(...)` calls
- `F.udf(...)` calls

Where `useArrow=True` or `@pandas_udf` is NOT used.

```python
# DETECT:
@udf(returnType=StringType())
def my_func(x):
    return x.upper()

transform = udf(lambda x: x * 2, IntegerType())
```

### What NOT to detect

```python
# DO NOT detect:
@pandas_udf(returnType=StringType())     # already uses Arrow
def my_func(x: pd.Series) -> pd.Series:
    ...
```

### AST detection approach

Walk `ast.FunctionDef` nodes with decorators. Check if any decorator is a `Call`
or `Name` matching `udf`. Also walk `ast.Call` nodes where the function matches
`udf`, `F.udf`, or `spark.udf.register`.

For `@udf` decorators: check decorator list of each `FunctionDef`.

### Output text

**message:** `Python UDF detected — consider pandas_udf for better performance`

**explanation:**
```
Python UDFs serialize each row to Python, execute Python, then deserialize back to
JVM. This row-at-a-time serialization overhead can make a UDF 10-100x slower than
an equivalent Spark built-in function. Catalyst cannot optimize through UDFs, so
query plans are less efficient.
```

**suggestion:**
```
First check if a Spark built-in function covers your use case (functions in
pyspark.sql.functions cover most common transformations).
If a custom function is necessary, use pandas_udf for vectorized execution:
  from pyspark.sql.functions import pandas_udf
  @pandas_udf(returnType=StringType())
  def my_func(x: pd.Series) -> pd.Series:
      return x.str.upper()
```

---

## SDK006 — Repartition to One

**Severity:** ERROR
**Title:** repartition(1) or coalesce(1) — forces single-partition execution

### What to detect

`repartition(1)` or `coalesce(1)` — literal integer 1 as first argument.

```python
# DETECT:
df.repartition(1)
df.coalesce(1)
df.repartition(1, "date")   # even with column arg, count=1 is wrong
```

### What NOT to detect

```python
# DO NOT detect if it's clearly intentional for writing a single file
# (we can't detect intent, so we flag it and explain the tradeoff in the suggestion)
```

### Output text

**message:** `repartition(1) forces all data through a single task`

**explanation:**
```
repartition(1) or coalesce(1) collapses the entire distributed dataset into a single
partition, processed by a single executor core. This eliminates all parallelism. On
a 100 GB dataset, one task processes all 100 GB sequentially. Memory pressure causes
spill to disk. Runtime scales linearly with data size.
```

**suggestion:**
```
If you need to write a single output file:
  df.write.option("maxRecordsPerFile", 1_000_000).parquet(path)
  # or
  df.coalesce(1).write.parquet(path)  # acceptable only for small DataFrames < 1 GB
If you genuinely need one partition for downstream logic, document why explicitly.
Most cases that use repartition(1) in production should not.
```

---

## SDK007 — Unpersisted Cache

**Severity:** INFO
**Title:** cache() or persist() without corresponding unpersist()

### What to detect

`df.cache()` or `df.persist()` calls in a scope where there is no corresponding
`df.unpersist()` call on the same variable name.

```python
# DETECT:
df.cache()
# ... use df ...
# no df.unpersist() anywhere in the file

result = df.cache()
# result never unpersisted
```

### What NOT to detect

```python
# DO NOT detect:
df.cache()
# ... use df ...
df.unpersist()    # properly released
```

### AST detection approach

This requires tracking variable names. Walk the AST and collect:
- All variable names that have `.cache()` or `.persist()` called on them
- All variable names that have `.unpersist()` called on them

For each cached variable with no corresponding unpersist in the same file,
emit a diagnostic. Use the line of the cache/persist call.

**Note:** This rule operates at file scope. It cannot track cross-file persistence.
Emit the diagnostic as INFO since there are legitimate cases (session-long caches).

### Output text

**message:** `cache()/persist() without unpersist() — cached data may leak memory`

**explanation:**
```
Spark holds cached DataFrames in executor memory (and optionally disk) until
explicitly released or the SparkSession ends. In long-running jobs or notebooks,
forgetting unpersist() accumulates cached data, consuming memory that other
operations need and potentially causing executor OOM.
```

**suggestion:**
```
Release cached DataFrames when they are no longer needed:
  df.cache()
  # ... operations that benefit from the cache ...
  df.unpersist()
In notebooks or long jobs, use a try/finally pattern:
  df.cache()
  try:
      process(df)
  finally:
      df.unpersist()
```

---

## AST Patterns Reference

Common patterns used across multiple rules:

```python
import ast

# Check if a call is `something.method_name(...)`
def is_method_call(node: ast.Call, method_name: str) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == method_name
    )

# Get the integer value of the first argument if it's a literal int
def first_arg_int(node: ast.Call) -> int | None:
    if node.args and isinstance(node.args[0], ast.Constant):
        v = node.args[0].value
        if isinstance(v, int):
            return v
    return None

# Walk only within a loop body (not the whole tree)
def calls_in_loop_body(loop: ast.For | ast.While) -> list[ast.Call]:
    calls = []
    for child in ast.walk(ast.Module(body=loop.body, type_ignores=[])):
        if isinstance(child, ast.Call):
            calls.append(child)
    return calls

# Get the receiver name of a method call (df.method() -> "df")
def receiver_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Attribute):
        if isinstance(node.func.value, ast.Name):
            return node.func.value.id
    return None
```
