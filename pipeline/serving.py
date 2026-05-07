"""Serving layer — the two read-side functions the tests and consumers call.

    total_revenue(d: str, h: int) -> float   # analyst query
    avg_revenue(h: int) -> float             # backend query

Both are expected to be fast — the stage-4 lesson spells out the budgets
and the split between the analyst store and the backend cache.

Config is read from `pipeline.config` so callers stay arg-less; tests set
env vars via fixtures.
"""
from __future__ import annotations


def total_revenue(d: str, h: int) -> float:
    raise NotImplementedError("Stage 3: implement total_revenue — see stages/03-etl-analyst-store/")


def avg_revenue(h: int) -> float:
    raise NotImplementedError("Stage 4: implement avg_revenue — see stages/04-backend-serving/")
