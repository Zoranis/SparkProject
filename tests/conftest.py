"""Test harness: session-scoped SparkSession + schema, per-test data cleanup.

Lifecycle:
  - session start: reset_all (drop schema, flush redis) + student's migrate()
  - before each test: clear_data (truncate tables, flush redis) — preserves schema
  - session end: reset_all (leave the DB empty)

Tests call `run_etl(spark, landing_dir, connections)` and the two serving
functions as black boxes. `connections` carries the plumbing; the storage
technology choice is visible via the `postgres*` / `redis` keys.

Environment expected (set by compose.yml / Makefile `test-spark` target):
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB (analyst store)
    REDIS_HOST, REDIS_PORT                        (serving store)
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

# Ensure /home/jovyan/work is on sys.path so `pipeline` / `helpers` import cleanly.
WORK = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(WORK))

from pyspark.sql import SparkSession  # noqa: E402

from pipeline import config  # noqa: E402
from pipeline.migrate import migrate  # noqa: E402
from pipeline.reset import clear_data, reset_all  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    import os
    s = (SparkSession.builder
        .appName("lab-tests")
        .master(os.environ.get("SPARK_TEST_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="session", autouse=True)
def _schema_lifecycle():
    """Create the schema once per test session; drop it at teardown."""
    pg = config.postgres_kwargs()
    rd = config.redis_kwargs()
    reset_all(pg, rd)
    migrate(pg)
    yield
    reset_all(pg, rd)


@pytest.fixture(autouse=True)
def _clean_data_between_tests():
    """Truncate tables and flush redis between tests. Schema stays."""
    clear_data(config.postgres_kwargs(), config.redis_kwargs())
    yield


@pytest.fixture()
def landing_dir(tmp_path: Path) -> Path:
    d = tmp_path / "landing"
    d.mkdir()
    return d


@pytest.fixture()
def connections(landing_dir: Path) -> dict:
    return {
        "paths": {
            "landing": str(landing_dir),
            "in_process": str(landing_dir.parent / "in_process"),
            "archive": str(landing_dir.parent / "archive"),
            "errors": str(landing_dir.parent / "errors"),
        },
        "postgres": config.postgres_kwargs(),
        "postgres_jdbc": config.postgres_jdbc(),
        "redis": config.redis_kwargs(),
    }


@pytest.fixture(autouse=True)
def _cleanup_bucket_dirs(landing_dir: Path):
    yield
    for name in ("in_process", "archive", "errors"):
        p = landing_dir.parent / name
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
