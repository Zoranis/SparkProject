"""Stage 4 — latency budgets.

- `total_revenue(d, h)`: ≤ 1s median over 20 trials.
- `avg_revenue(h)`:      ≤ 50ms median over 50 trials (ms-budget).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from helpers.test_utils import make_trip_row, median_latency, write_day_fixture
from pipeline.etl import run_etl
from pipeline.serving import avg_revenue, total_revenue

TOTAL_REVENUE_BUDGET_S = 1.0
AVG_REVENUE_BUDGET_S = 0.050  # 50ms median


def _seed(landing: Path) -> None:
    # Populate several days at the same hour so avg_revenue(h) has something to average.
    for day in ("2019-06-01", "2019-06-02", "2019-06-03"):
        write_day_fixture(landing, day, [
            make_trip_row(f"{day} 08:15:00", total_amount=10.0),
            make_trip_row(f"{day} 08:45:00", total_amount=20.0),
        ])


@pytest.fixture()
def populated(spark, landing_dir, connections):
    _seed(landing_dir)
    run_etl(spark, str(landing_dir), connections)
    return connections


def test_total_revenue_latency(populated):
    median = median_latency(lambda: total_revenue("2019-06-01", 8), trials=20)
    assert median <= TOTAL_REVENUE_BUDGET_S, f"median={median:.3f}s exceeds {TOTAL_REVENUE_BUDGET_S}s budget"


def test_avg_revenue_latency(populated):
    median = median_latency(lambda: avg_revenue(8), trials=50)
    assert median <= AVG_REVENUE_BUDGET_S, f"median={median*1000:.1f}ms exceeds {AVG_REVENUE_BUDGET_S*1000:.0f}ms budget"
