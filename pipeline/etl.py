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

from pathlib import Path

import psycopg2
import redis
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_etl(
    spark_session: SparkSession,
    input_file_dir: str,
    connections: dict,
) -> None:
    pg = connections["postgres"]
    landing = Path(input_file_dir)

    all_files = {f.name: f for f in sorted(landing.glob("*.parquet"))}
    if not all_files:
        return

    with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
        cur.execute("SELECT filename FROM processed_files")
        processed = {row[0] for row in cur.fetchall()}

    new_files = {name: path for name, path in all_files.items() if name not in processed}
    if not new_files:
        return

    for filename, filepath in new_files.items():
        df = spark_session.read.parquet(str(filepath))

        agg_rows = (
            df
            .withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("hour", F.hour("tpep_pickup_datetime"))
            .groupBy("trip_date", "hour")
            .agg(F.sum("total_amount").alias("total_amount"))
            .collect()
        )

        with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
            for row in agg_rows:
                cur.execute(
                    """
                    INSERT INTO hourly_revenue (trip_date, hour, total_amount)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (trip_date, hour)
                        DO UPDATE SET total_amount = EXCLUDED.total_amount
                    """,
                    (row["trip_date"], row["hour"], float(row["total_amount"])),
                )
            cur.execute(
                "INSERT INTO processed_files (filename) VALUES (%s) ON CONFLICT DO NOTHING",
                (filename,),
            )

    # Refresh Redis cache: recompute avg_revenue(h) for all hours from the source of truth.
    rd = connections["redis"]
    r = redis.Redis(**rd)
    with psycopg2.connect(**pg) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT hour, AVG(total_amount) FROM hourly_revenue GROUP BY hour"
        )
        for hour, avg in cur.fetchall():
            r.set(f"avg_revenue:{hour}", float(avg))
