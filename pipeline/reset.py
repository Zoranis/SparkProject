"""Infrastructure — do not modify as a student.

Two functions, same contract as scaffold's `ecommerce_pipeline.reset`:

  reset_all(pg_kwargs, redis_kwargs)
      Drop every student-owned object in the Postgres `public` schema and
      flush Redis. Use before `migrate()` to start from a blank slate.

  clear_data(pg_kwargs, redis_kwargs)
      Truncate every table in `public` (preserving structure + indexes) and
      flush Redis. Faster than reset_all + migrate — used between tests so
      the schema is built once per session, not per test.
"""
from __future__ import annotations

import psycopg2
import redis


def reset_all(pg_kwargs: dict, redis_kwargs: dict) -> None:
    with psycopg2.connect(**pg_kwargs) as conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
    redis.Redis(**redis_kwargs).flushdb()


def clear_data(pg_kwargs: dict, redis_kwargs: dict) -> None:
    with psycopg2.connect(**pg_kwargs) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
        tables = [row[0] for row in cur.fetchall()]
        for t in tables:
            cur.execute(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE')
    redis.Redis(**redis_kwargs).flushdb()
