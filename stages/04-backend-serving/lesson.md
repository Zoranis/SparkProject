---
kernelspec:
  name: python3
  language: python
  display_name: Python 3
---

# Stage 4 — Backend Serving Layer

## The Situation

A backend service on a user-facing path needs `avg_revenue(h)` — the average
hourly revenue for hour-of-day *h*, across the whole history. It's called
many times per second. The budget is a few **milliseconds**.

The analyst store from stage 3 can probably answer this query correctly. It
cannot answer it fast enough. Even the best index plan has overhead your
user-facing path can't afford.

## Design Considerations

- How many distinct answers does `avg_revenue(h)` ever produce? (Think in
  cardinality — the answer is very small.)
- Where should the pre-computed answer live, and who updates it?
- When `run_etl` adds a new hour's worth of revenue to the analyst store,
  what has to happen for `avg_revenue(h)` to stay correct?
- If the pre-computed answer goes briefly stale, what's the worst case?

The shape of the store you pick should make the millisecond budget obvious —
not something you have to fight for with indexes.

## What to Build

Implement in `pipeline/serving.py`:

```python
def avg_revenue(h: int) -> float:
    ...
```

Extend `run_etl` so whatever this function reads from stays consistent with
the analyst store. Think about idempotency again: if a batch is replayed,
`avg_revenue(h)` must not drift.

## Definition of Done

```bash
docker exec spark-jupyter pytest tests/test_stage4_latency.py -v
```

- Correctness: `avg_revenue(h)` returns the expected value for the fixture.
- Latency (median of 50, one warmup): `avg_revenue(h)` under 50ms,
  `total_revenue(d, h)` still under 1s.

## Before You Move On

- What happens on cold start of the serving store? Who repopulates it?
- Is your `avg_revenue` additive across batches, or does it recompute from
  scratch each tick? What does that imply if a batch is processed twice?
