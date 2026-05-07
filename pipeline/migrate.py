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


def migrate(pg_kwargs: dict) -> None:
    raise NotImplementedError("Stage 3: implement migrate — see stages/03-etl-analyst-store/")
