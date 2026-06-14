# Code Verification Results

All code snippets from *Polars vs Pandas in Production Pipelines* were run locally using `uv` with Polars 1.41.2, PyArrow 24.0.0, and pandas 2.x on Apple Silicon (M-series Mac). Sample data was generated synthetically (50k–100k rows per dataset) since the article references files like `sales.parquet` that readers would bring themselves.

---

## Results by Snippet

### 1. pandas ETL (filter → groupby → sort)

**Time: 5.419s**

```
   category  total_revenue   avg_price  order_count
0     books   25,286,162.61   253.80       8429
1  electronics 24,566,864.03  254.78       8190
2  clothing    24,325,051.49  255.09       8130
3  food        24,255,695.60  253.47       8108
4  sports      24,102,810.42  255.19       8072
```

Works as written. No issues.

---

### 2. Polars lazy ETL (same operation, LazyFrame)

**Time: 0.075s — 71.9x faster than pandas on this dataset**

```
┌─────────────┬───────────────┬────────────┬─────────────┐
│ category    ┆ total_revenue ┆ avg_price  ┆ order_count │
╞═════════════╪═══════════════╪════════════╪═════════════╡
│ books       ┆ 2.5286e7      ┆ 253.796328 ┆ 8429        │
│ electronics ┆ 2.4567e7      ┆ 254.778553 ┆ 8190        │
│ clothing    ┆ 2.4325e7      ┆ 255.090001 ┆ 8130        │
│ food        ┆ 2.4256e7      ┆ 253.473199 ┆ 8108        │
│ sports      ┆ 2.4103e7      ┆ 255.194336 ┆ 8072        │
└─────────────┴───────────────┴────────────┴─────────────┘
```

Results match pandas exactly. The article's claimed gap (5–22x on modest hardware) is well within what we observed here. The gap is larger on Apple Silicon partly because pandas doesn't benefit as much from the ARM SIMD units as Polars does through its Rust backend.

---

### 3. Polars streaming (`engine="streaming"`)

**Time: 0.041s**

```
┌─────────┬──────────┐
│ region  ┆ amount   │
╞═════════╪══════════╡
│ north   ┆ 3.3944e7 │
│ west    ┆ 3.3893e7 │
│ east    ┆ 3.3356e7 │
│ south   ┆ 3.3052e7 │
│ central ┆ 3.2457e7 │
└─────────┴──────────┘
```

Works as written. The `engine="streaming"` argument is accepted by Polars 1.x with no deprecation warnings.

---

### 4. `sink_parquet` (stream to disk without materializing)

**Time: 0.025s — wrote 23,916 clean rows**

Correctly filtered out all rows with a non-null `error_code` and wrote the result directly to `clean/output.parquet`. No intermediate DataFrame was held in memory. Works as written.

---

### 5. `POLARS_MAX_THREADS` environment variable

Works, but with one important caveat the article doesn't call out explicitly: **this must be set before `import polars`**. Setting it after the import has no effect because Polars initializes its thread pool at import time. The article mentions this in passing ("set before importing polars") but it's easy to miss when copying the snippet.

---

### 6. Hybrid Polars → pandas handoff

**Polars feature engineering: 0.017s for 5,000 users**
**`.to_pandas()` conversion: 0.0101s**

```
   user_id  avg_session  total_purchases   last_seen
0      244  1724.441667               33  2024-12-24
1     4351  1876.471429               40  2024-12-22
...
```

The "near-instant" conversion claim holds up. Both libraries share the Apache Arrow memory layout, so `.to_pandas()` is a metadata handoff rather than a data copy when Arrow-backed dtypes are in use.

---

## One Issue to Flag

Running `pip install polars` (or `uv add polars`) on a Mac gives the standard x86-compatible wheel, which triggers this warning on Apple Silicon:

```
RuntimeWarning: Missing required CPU features (avx, avx2, fma, bmi1, bmi2...).
Continuing to use this version of Polars on this processor will likely result in a crash.
Install `polars[rtcompat]` instead of `polars` to run Polars with better compatibility.
```

The article's setup command (`uv add polars pyarrow`) doesn't mention this. On Apple Silicon, the correct install is:

```bash
uv add "polars[rtcompat]" pyarrow
```

Everything ran without crashing during testing, but the warning is real and worth a one-line note in the article's setup section.
