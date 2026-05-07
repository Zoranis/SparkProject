---
kernelspec:
  name: python3
  language: python
  display_name: Python 3
---

# Stage 5 — Bonus

Pick one (or both). Same stage-4 tests must still pass.

## Option A — Serving store as the source of truth for `avg_revenue`

Today your `avg_revenue` almost certainly reads from a pre-computed value
sitting in the serving store. That value was derived from the analyst store.

Can you make `run_etl` *maintain* the `avg_revenue` answer directly in the
serving store — incrementing it as each new batch arrives — and never look at
the analyst store to compute it?

Things to think about:

- Addition is associative, but associativity is not idempotency. If a batch
  is written twice, what guard do you need?
- Are you keeping running sums and counts, or pre-averaged values? Why?
- What is the cold-start story when the serving store is empty?

## Option B — Crash-midway recovery

Kill the scheduler with `docker kill --signal=SIGKILL spark-producer` during
a run, then bring it back up. Can you prove that your aggregates converge to
the correct values within N ticks? Write a test that demonstrates it.
