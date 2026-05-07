"""Test helpers shared across stage test files.

Two responsibilities:
  1. Fabricate tiny daily-parquet fixtures with known totals (so tests can assert
     against hand-computed expected values).
  2. Measure latency with a warmup + median-of-N pattern so the budget assertion
     survives a noisy first call (JIT, connection setup, cache warming).
"""
from __future__ import annotations

import statistics
import time
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd


def write_day_fixture(
    dest_dir: Path,
    day: str,
    rows: Iterable[dict],
) -> Path:
    """Write a single-day parquet file matching the TLC schema subset the ETL reads."""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(list(rows))
    out = dest_dir / f"{day}.parquet"
    df.to_parquet(out, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    return out


def make_trip_row(
    pickup_ts: str,
    pu_location_id: int = 1,
    total_amount: float = 10.0,
    fare_amount: float = 8.0,
    tip_amount: float = 2.0,
) -> dict:
    return {
        "tpep_pickup_datetime": pd.Timestamp(pickup_ts),
        "PULocationID": int(pu_location_id),
        "total_amount": float(total_amount),
        "fare_amount": float(fare_amount),
        "tip_amount": float(tip_amount),
    }


def median_latency(
    fn: Callable[[], object],
    *,
    trials: int = 50,
    warmup: int = 1,
) -> float:
    """Call `fn` once as warmup, then N more times. Return the median wall-clock in seconds."""
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(trials):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)
