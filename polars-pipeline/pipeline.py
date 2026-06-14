"""
Runs all code snippets from the article:
"Polars vs Pandas in Production Pipelines"
"""

import os
import pathlib
import random
import string

# ── 0. Generate sample data ──────────────────────────────────────────────────

import pyarrow as pa
import pyarrow.parquet as pq

print("=" * 60)
print("Generating sample data...")
print("=" * 60)

rng = random.Random(42)
categories = ["electronics", "clothing", "food", "books", "sports"]
regions = ["north", "south", "east", "west", "central"]
statuses = ["active", "inactive", "pending"]

def make_sales_table(n=50_000):
    return pa.table({
        "order_id": list(range(n)),
        "category": [rng.choice(categories) for _ in range(n)],
        "revenue": [round(rng.uniform(100, 5000), 2) for _ in range(n)],
        "price": [round(rng.uniform(10, 500), 2) for _ in range(n)],
    })

def make_large_table(n=100_000):
    return pa.table({
        "status": [rng.choice(statuses) for _ in range(n)],
        "region": [rng.choice(regions) for _ in range(n)],
        "amount": [round(rng.uniform(1, 10_000), 2) for _ in range(n)],
    })

def make_raw_table(n=40_000):
    error_codes = [None, None, None, "E001", "E002"]
    return pa.table({
        "error_code": [rng.choice(error_codes) for _ in range(n)],
        "value": [rng.random() for _ in range(n)],
    })

def make_events_table(n=60_000):
    import datetime
    base = datetime.date(2024, 1, 1)
    return pa.table({
        "user_id": [rng.randint(1, 5_000) for _ in range(n)],
        "session_duration": [round(rng.uniform(10, 3600), 1) for _ in range(n)],
        "purchase": [rng.randint(0, 5) for _ in range(n)],
        "event_date": [(base + datetime.timedelta(days=rng.randint(0, 364))).isoformat()
                       for _ in range(n)],
    })

pq.write_table(make_sales_table(), "sales.parquet")

pathlib.Path("large_dataset").mkdir(exist_ok=True)
pq.write_table(make_large_table(), "large_dataset/data.parquet")

pathlib.Path("raw").mkdir(exist_ok=True)
pathlib.Path("clean").mkdir(exist_ok=True)
pq.write_table(make_raw_table(), "raw/data.parquet")

pathlib.Path("events").mkdir(exist_ok=True)
pq.write_table(make_events_table(), "events/data.parquet")

print("Sample files created: sales.parquet, large_dataset/, raw/, events/")
print()

# ── 1. pandas ETL ────────────────────────────────────────────────────────────

print("=" * 60)
print("SNIPPET 1 — pandas version (filter + groupby + sort)")
print("=" * 60)

import pandas as pd
import time

start = time.perf_counter()

df = pd.read_parquet("sales.parquet")
result = (
    df[df["revenue"] > 1000]
    .groupby("category")
    .agg(
        total_revenue=("revenue", "sum"),
        avg_price=("price", "mean"),
        order_count=("order_id", "count"),
    )
    .sort_values("total_revenue", ascending=False)
    .reset_index()
)

pandas_time = time.perf_counter() - start
print(f"pandas: {pandas_time:.3f}s")
print(result)
print()

# ── 2. Polars ETL ────────────────────────────────────────────────────────────

print("=" * 60)
print("SNIPPET 2 — Polars version (lazy, predicate + projection pushdown)")
print("=" * 60)

import polars as pl

start = time.perf_counter()

result_pl = (
    pl.scan_parquet("sales.parquet")          # no data read yet
    .filter(pl.col("revenue") > 1000)         # pushed into the scan
    .group_by("category")
    .agg(
        total_revenue=pl.col("revenue").sum(),
        avg_price=pl.col("price").mean(),
        order_count=pl.col("order_id").count(),
    )
    .sort("total_revenue", descending=True)
    .collect()                                 # execution happens here
)

polars_time = time.perf_counter() - start
print(f"Polars: {polars_time:.3f}s")
print(result_pl)
print(f"\nSpeedup: {pandas_time / polars_time:.1f}x faster than pandas")
print()

# ── 3. Streaming on larger-than-memory data ───────────────────────────────────

print("=" * 60)
print("SNIPPET 3 — Polars streaming (collect with engine='streaming')")
print("=" * 60)

start = time.perf_counter()

result_stream = (
    pl.scan_parquet("large_dataset/*.parquet")
    .filter(pl.col("status") == "active")
    .group_by("region")
    .agg(pl.col("amount").sum())
    .sort("amount", descending=True)
    .collect(engine="streaming")              # out-of-core execution
)

print(f"Polars streaming: {time.perf_counter() - start:.3f}s")
print(result_stream)
print()

# ── 4. sink_parquet (stream directly to disk) ────────────────────────────────

print("=" * 60)
print("SNIPPET 4 — sink_parquet (streams to disk without materializing)")
print("=" * 60)

start = time.perf_counter()

(
    pl.scan_parquet("raw/*.parquet")
    .filter(pl.col("error_code").is_null())
    .sink_parquet("clean/output.parquet")     # streams to disk in batches
)

elapsed = time.perf_counter() - start
rows_out = pl.read_parquet("clean/output.parquet").shape[0]
print(f"sink_parquet: {elapsed:.3f}s — wrote {rows_out:,} clean rows to clean/output.parquet")
print()

# ── 5. Production guardrail: pin thread count ─────────────────────────────────

print("=" * 60)
print("SNIPPET 5 — POLARS_MAX_THREADS (must be set before import)")
print("=" * 60)

# Note: polars is already imported, so this affects new thread-pool operations
# In production you'd set this before any polars import.
os.environ["POLARS_MAX_THREADS"] = "8"
print(f"POLARS_MAX_THREADS set to: {os.environ['POLARS_MAX_THREADS']}")
print("(In production: set this env var before importing polars)")
print()

# ── 6. Hybrid Polars → pandas handoff for ML ────────────────────────────────

print("=" * 60)
print("SNIPPET 6 — Hybrid: Polars heavy compute → pandas for ML boundary")
print("=" * 60)

start = time.perf_counter()

features = (
    pl.scan_parquet("events/*.parquet")
    .group_by("user_id")
    .agg([
        pl.col("session_duration").mean().alias("avg_session"),
        pl.col("purchase").sum().alias("total_purchases"),
        pl.col("event_date").max().alias("last_seen"),
    ])
    .collect(engine="streaming")
)

polars_part = time.perf_counter() - start
print(f"Polars feature engineering: {polars_part:.3f}s — {features.shape[0]:,} users")
print(features.head())

# Zero-copy conversion at the ML boundary
start = time.perf_counter()
X = features.to_pandas()
print(f"\nto_pandas() conversion: {time.perf_counter() - start:.4f}s (Arrow-backed, near-instant)")
print(X.head())
print(f"\nDataFrame type: {type(X).__name__}, shape: {X.shape}")
print()

print("=" * 60)
print("All snippets completed successfully.")
print("=" * 60)
