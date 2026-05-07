---
kernelspec:
  name: python3
  language: python
  display_name: Python 3
---

# Stage 2 — Incremental Ingest

## The Situation

The scheduler fires `run_etl` every 60 seconds. In that minute, the producer
has dropped roughly 6 new daily files into `data/landing/`. Everything that
landed *before* the previous tick is still sitting there too — nothing cleans
the directory unless your ETL does it.

If you process the whole landing directory every tick, you do the same work
over and over, and the analyst store ends up with duplicates.

**Contract:** `run_etl` must be **incremental** — running it a second time
with no new files in the landing directory must do no useful work and must
not change the analyst store.

## Design Considerations

Before you start implementing, think through:

- How does a process know, on tick N, which files are new versus which it has
  already seen? There's more than one reasonable answer — we'll discuss the
  trade-offs in class.
- What happens if the process crashes after consuming a file but before
  recording that it consumed it? What happens on the next tick?
- What happens if two copies of `run_etl` run at the same time?

## What to Build

Open `pipeline/etl.py` and implement:

```python
def run_etl(spark_session, input_file_dir, connections):
    ...
```

The function may create any structures it likes inside the databases reachable
from `connections`. Tests do not inspect those structures directly — they
only check behavior at the analyst store (`total_revenue(d, h)`).

## Definition of Done

- `run_etl` picks up only the files that are new since the last successful run.
- On a repeat call with no new files, the analyst store is unchanged.
- The test file drives it:

```bash
docker exec spark-jupyter pytest tests/test_stage2_incrementality.py -v
```

`total_revenue` is the canary — the test seeds two files, runs `run_etl`,
reads `total_revenue(d, h)`, runs `run_etl` again, reads the same cell, and
asserts the value did not change (and matches the hand-computed expected).

## Before You Move On

- Could two `run_etl` processes overlap? If yes, what would happen? If you're
  relying on them not overlapping, what in the system enforces that?
- What if a daily file arrives in landing that was *already* processed
  yesterday (a genuine replay, not a bug)? Does your design handle it, or is
  that a stage 3 problem?
