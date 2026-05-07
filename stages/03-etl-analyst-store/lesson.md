---
kernelspec:
  name: python3
  language: python
  display_name: Python 3
---

# Stage 3 — Analyst Store and Idempotency

## The Situation

The analyst wants `total_revenue(d, h)` — "how much revenue did we make in
hour *h* of day *d*?". The budget is ≤ 1 second per call, and the analyst is
fine with batch-scale freshness (a few minutes behind is OK).

You already have incremental ingest (stage 2). Now you need to *produce*
the aggregates the analyst reads, and hold up a stronger guarantee:
**idempotency**.

## Incrementality vs Idempotency

These look similar but are different problems.

- **Incrementality** (stage 2) — don't reprocess files you've already consumed.
- **Idempotency** (this stage) — if the same batch of records ends up written
  more than once (because a previous run failed halfway, or because the
  scheduler retried), the final state of the analyst store must be the same
  as if it had been written once.

We'll discuss in class when a simple guard on stage 2 is enough, and when
you need idempotent writes on top of it.

## Design Considerations

- What is the grain of the analyst aggregate? (What keys the row?)
- When a replay happens, what primitive does your store give you so the second
  write doesn't end up doubling the numbers?
- What index or access pattern does `total_revenue(d, h)` need to stay under
  a 1-second budget?

## What to Build

Three files move in this stage:

1. `pipeline/migrate.py::migrate` — codify the analyst-store schema you
   picked above. `migrate` is called once before the ETL runs and must be
   idempotent (running it twice in a row leaves the same schema).
2. `pipeline/etl.py::run_etl` — extend it so that after ingesting new
   files, it writes/updates the aggregates the analyst will read.
3. `pipeline/serving.py::total_revenue`:

```python
def total_revenue(d: str, h: int) -> float:
    ...
```

`d` is `YYYY-MM-DD`. `h` is an integer 0..23. Return total revenue (sum of
`total_amount`) across all zones for that hour of that day.

## Definition of Done

- `total_revenue(d, h)` returns the correct hand-computed value for the
  fixture in `tests/test_stage3_idempotency.py`.
- `run_etl` called twice (with or without new files) leaves `total_revenue`
  readings unchanged.
- Latency of `total_revenue` stays under 1s (checked in stage 4).

```bash
docker exec spark-jupyter pytest tests/test_stage3_idempotency.py -v
```

## Before You Move On

- Look at an `EXPLAIN` for the query that backs `total_revenue`. Is the plan
  seeking or scanning?
- If the analyst added `zone_id` to the query tomorrow, does your grain and
  index hold up?
