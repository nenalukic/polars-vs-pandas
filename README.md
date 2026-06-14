# Polars vs Pandas in Production Pipelines

Companion code for the article *Polars vs Pandas in Production Pipelines*.

Runs every code snippet from the article end-to-end — pandas ETL, Polars lazy execution, streaming, `sink_parquet`, thread config, and the Polars → pandas ML handoff — using synthetically generated data so you can reproduce the results without needing your own dataset.

## Setup

Requires [uv](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:nenalukic/-polars-vs-pandas.git
cd -polars-vs-pandas/polars-pipeline
uv run pipeline.py
```

> **Apple Silicon (M1/M2/M3/M4):** If you see a CPU features warning, replace `polars` with `polars[rtcompat]` in `pyproject.toml` and re-run `uv sync`.

## What the script runs

| Snippet | What it demonstrates |
|---|---|
| 1 | pandas: `read_parquet` → filter → `groupby` → `sort_values` |
| 2 | Polars: `scan_parquet` → `.filter` → `.group_by` → `.sort` → `.collect` |
| 3 | Polars streaming: `collect(engine="streaming")` for larger-than-memory data |
| 4 | `sink_parquet`: stream results directly to disk without materializing |
| 5 | `POLARS_MAX_THREADS`: thread count pinning for production environments |
| 6 | Hybrid pattern: Polars feature engineering → `.to_pandas()` for ML handoff |

## Results

Full output with timings and observations in [`polars-pipeline/results.md`](polars-pipeline/results.md).

Key numbers on Apple Silicon M-series (50k row dataset):

| Operation | Time |
|---|---|
| pandas ETL | 5.419s |
| Polars lazy ETL | 0.075s (**71.9x faster**) |
| Polars streaming | 0.041s |
| `sink_parquet` | 0.025s |
| `.to_pandas()` conversion | 0.010s |

The 71.9x gap is larger than the article's stated 5–22x range for modest hardware — Apple Silicon benefits more from Polars' ARM-optimized Rust backend than pandas does.

## Dependencies

- [`polars`](https://pola.rs) 1.41.2
- [`pyarrow`](https://arrow.apache.org/docs/python/) 24.0.0
- [`pandas`](https://pandas.pydata.org) 2.x
