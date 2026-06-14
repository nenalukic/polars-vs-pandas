# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

This is a blog content repository for a single technical article: **"Polars vs Pandas in Production Pipelines"**. It contains no executable code, build system, or tests.

## Contents

- `Polars vs Pandas in Production Pipelines.md` — the main article in Markdown
- `thumbnail.png` — cover/thumbnail image
- `visual 1.png`, `visual 2.png`, `visual 3.png` — diagrams referenced inline in the article via `![][image1]`, `![][image2]`, `![][image3]` placeholder syntax

## Article Structure

The article is organized as:
1. Motivation — why pandas becomes a bottleneck at scale
2. Execution model comparison — pandas (NumPy/GIL/eager) vs Polars (Rust/Arrow/lazy)
3. Benchmark evidence — PDS-H results, Check Technologies migration, DB Systel case study
4. Setup and side-by-side code examples
5. Migration guidance

## Conventions

- Image references use Google Docs export syntax (`![][image1]`) rather than standard Markdown image paths — the actual images are the numbered PNG files in the repo root.
- Code blocks in the article use escaped underscores and backslashes (artifact of Google Docs export); when editing, preserve this style or normalize to standard Markdown fences.
