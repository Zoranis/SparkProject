---
kernelspec:
  name: python3
  language: python
  display_name: Python 3
---

# Stage 1 — Exploration

## The Situation

The producer is dropping one daily parquet file into `data/landing/` every 10
seconds. Before you design anything, you need to understand the data you're
about to pipe through.

For this stage, don't touch the live landing directory. There's a pre-seeded
`data/sandbox/` with 10 daily files that sits still — perfect for poking at.

## What to Produce

Open `notebooks/00-exploration.ipynb`. Answer these — in the notebook or in a
short note to yourself:

1. **Row shape per file.** How many rows does a single day typically have?
   What columns are in the raw schema, which of them do you actually need for
   the two consumer queries, and which can you drop on the way in?
2. **Landing-side throughput.** One file every 10s means six files per minute.
   Multiply through — roughly how many raw rows per minute is the landing rate?
3. **Analyst-grain ceiling.** The analyst asks `total_revenue(d, h)`. The
   backend asks `avg_revenue(h)`. What's the *upper bound* on row count of an
   aggregate table keyed at `(day, hour, pickup_zone)`? (Hint: 365 days × 24
   hours × 265 zones.) Compare to the landing-side throughput.
4. **What to store, where.** Given (2) and (3) — where does raw land, where do
   aggregates live, and does one store plausibly do both? You don't need to
   pick a technology yet; you need a picture of the sizes involved.

## No Tests Here

This stage has no pytest suite. The output is your understanding — bring it to
the class discussion. You'll see those numbers again in stage 2 and stage 3.

## When You're Done

Write a sentence or two for each of the four questions. Move on to
`stages/02-incremental-ingest/lesson.md`.
