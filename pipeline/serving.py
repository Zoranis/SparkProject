"""Serving layer — the two read-side functions the tests and consumers call.

    total_revenue(d: str, h: int) -> float   # analyst query
    avg_revenue(h: int) -> float             # backend query

Both are expected to be fast — the stage-4 lesson spells out the budgets
and the split between the analyst store and the backend cache.

Config is read from `pipeline.config` so callers stay arg-less; tests set
env vars via fixtures.
"""
from __future__ import annotations

import psycopg2

from pipeline import config


def total_revenue(d: str, h: int) -> float:
    pg = config.postgres_kwargs()
    with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT total_amount FROM hourly_revenue WHERE trip_date = %s AND hour = %s",
            (d, h),
        )
        row = cur.fetchone()
    return float(row[0]) if row else 0.0


def avg_revenue(h: int) -> float:
    import redis as _redis
    rd = config.redis_kwargs()
    r = _redis.Redis(**rd)
    val = r.get(f"avg_revenue:{h}")
    return float(val) if val is not None else 0.0
