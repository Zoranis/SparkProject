"""Stage 3 — correctness + idempotency of the analyst store.

Correctness: hand-computed aggregate against a small fixture.
Idempotency: calling `run_etl` twice must not change the store's state.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from helpers.test_utils import make_trip_row, write_day_fixture
from pipeline.etl import run_etl
from pipeline.serving import total_revenue


def _seed(landing: Path) -> None:
    write_day_fixture(landing, "2019-03-10", [
        make_trip_row("2019-03-10 14:00:00", total_amount=12.5),
        make_trip_row("2019-03-10 14:30:00", total_amount=7.5),
        make_trip_row("2019-03-10 15:00:00", total_amount=100.0),
    ])


def test_total_revenue_correct(spark, landing_dir, connections):
    _seed(landing_dir)
    run_etl(spark, str(landing_dir), connections)
    assert total_revenue("2019-03-10", 14) == pytest.approx(20.0, rel=1e-6)
    assert total_revenue("2019-03-10", 15) == pytest.approx(100.0, rel=1e-6)


def test_run_etl_is_idempotent(spark, landing_dir, connections):
    _seed(landing_dir)
    run_etl(spark, str(landing_dir), connections)
    snapshot = total_revenue("2019-03-10", 14)

    # Second run — should leave the analyst store unchanged.
    run_etl(spark, str(landing_dir), connections)
    assert total_revenue("2019-03-10", 14) == pytest.approx(snapshot, rel=1e-6)
