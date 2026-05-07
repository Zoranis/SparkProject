"""ETL pipeline — the write side of the lab.

Contract (called by the scheduler and the stage tests):

    run_etl(spark_session, input_file_dir, connections) -> None

`connections` is a dict with keys `paths`, `postgres`, `redis` — see
`pipeline/config.py` for how the scheduler and tests assemble it.

Prerequisite: `pipeline.migrate.migrate` has already prepared the analyst
store — this function assumes the schema is in place.

Invariants the stage tests enforce:
  - Incremental (stage 2): calling `run_etl` a second time over the same
    state must be a no-op — the tests drop new files between calls and
    expect only the new rows to be processed.
  - Idempotent (stage 3): the per-(day, hour) aggregate that the analyst
    query reads must match the ground truth computed directly from the
    parquet input, regardless of how many batches it arrived in.
  - Latency (stage 4): the two consumer queries in `pipeline.serving`
    must stay fast after this function returns — see the budgets in the
    stage-4 lesson.

See `stages/02-incremental-ingest/`, `stages/03-etl-analyst-store/`, and
`stages/04-backend-serving/` for the requirements that drive this module.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def run_etl(
    spark_session: SparkSession,
    input_file_dir: str,
    connections: dict,
) -> None:
    raise NotImplementedError("Stage 2: implement run_etl — see stages/02-incremental-ingest/")
