"""Stage 2 — incrementality.

Running `run_etl` twice over the same landing state must not reprocess files.
We verify this at the analyst store: the post-2nd-run aggregate for any (d, h)
cell equals the post-1st-run aggregate (no double-counting).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from helpers.test_utils import make_trip_row, write_day_fixture
from pipeline.etl import run_etl
from pipeline.serving import total_revenue


def _seed_landing(landing: Path) -> None:
    write_day_fixture(landing, "2019-01-01", [
        make_trip_row("2019-01-01 05:12:00", total_amount=10.0),
        make_trip_row("2019-01-01 05:47:00", total_amount=20.0),
    ])
    write_day_fixture(landing, "2019-01-02", [
        make_trip_row("2019-01-02 09:10:00", total_amount=15.0),
    ])


def test_run_etl_is_incremental(spark, landing_dir, connections):
    _seed_landing(landing_dir)

    run_etl(spark, str(landing_dir), connections)
    after_first = total_revenue("2019-01-01", 5)

    # No new files added → second call must be a no-op against the analyst store.
    run_etl(spark, str(landing_dir), connections)
    after_second = total_revenue("2019-01-01", 5)

    assert after_first == pytest.approx(30.0, rel=1e-6)
    assert after_second == pytest.approx(after_first, rel=1e-6)
