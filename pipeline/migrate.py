"""Student-owned database migration.

Contract (do not rename the function or change the signature):

    migrate(pg_kwargs: dict) -> None

Called once before the ETL runs — from `pytest` (session setup) and from
`scripts/producer.py start`. Must be idempotent: running it twice in a row
must leave the schema in the same shape.

Implementation is free: plain SQL via psycopg2, alembic, or any migration
framework. The contract is purely the end state — after `migrate()` returns,
`run_etl` must be able to UPSERT per-(day, hour) revenue aggregates and
`total_revenue(d, h)` must return in ≤ 1s.

Redis has no schema, so it's not plumbed here — see `pipeline/reset.py` for
the wipe-side counterpart that touches both stores.
"""
from __future__ import annotations

import psycopg2


def migrate(pg_kwargs: dict) -> None:
    with psycopg2.connect(**pg_kwargs) as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                filename TEXT PRIMARY KEY
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hourly_revenue (
                trip_date DATE NOT NULL,
                hour      INT  NOT NULL,
                total_amount DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (trip_date, hour)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS hourly_revenue_date_hour_idx
                ON hourly_revenue (trip_date, hour)
        """)
