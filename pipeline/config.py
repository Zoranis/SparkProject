"""Env-driven config for the pipeline. Instructor-owned — do not edit as a student.

The module exposes neutral names (paths, budgets, connection URLs) so that neither
the stage lessons nor the student stubs have to hardcode any storage technology.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    landing: str
    in_process: str
    archive: str
    errors: str
    sandbox: str


@dataclass(frozen=True)
class Budgets:
    total_revenue_seconds: float = 1.0
    avg_revenue_seconds: float = 0.05


def paths() -> Paths:
    return Paths(
        landing=os.environ.get("LANDING_DIR", "/home/jovyan/work/data/landing"),
        in_process=os.environ.get("IN_PROCESS_DIR", "/home/jovyan/work/data/in_process"),
        archive=os.environ.get("ARCHIVE_DIR", "/home/jovyan/work/data/archive"),
        errors=os.environ.get("ERRORS_DIR", "/home/jovyan/work/data/errors"),
        sandbox=os.environ.get("SANDBOX_DIR", "/home/jovyan/work/data/sandbox"),
    )


def postgres_kwargs() -> dict:
    return {
        "host": os.environ.get("PG_HOST", "spark-postgres"),
        "port": int(os.environ.get("PG_PORT", "5432")),
        "dbname": os.environ.get("PG_DB", "taxi"),
        "user": os.environ.get("PG_USER", "spark"),
        "password": os.environ.get("PG_PASSWORD", "spark"),
    }


def postgres_jdbc() -> dict:
    return {
        "url": os.environ.get("PG_URL", "jdbc:postgresql://spark-postgres:5432/taxi"),
        "user": os.environ.get("PG_USER", "spark"),
        "password": os.environ.get("PG_PASSWORD", "spark"),
        "driver": "org.postgresql.Driver",
    }


def redis_kwargs() -> dict:
    return {
        "host": os.environ.get("REDIS_HOST", "spark-redis"),
        "port": int(os.environ.get("REDIS_PORT", "6379")),
    }
