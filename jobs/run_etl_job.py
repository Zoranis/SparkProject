"""spark-submit entrypoint. Instructor-owned.

Builds the SparkSession, assembles the `connections` dict from environment
config, and calls `pipeline.etl.run_etl`. No business logic lives here.

    spark-submit --master spark://spark-master:7077 \\
        /home/jovyan/work/jobs/run_etl_job.py
"""
from __future__ import annotations

import os
import sys

# Ensure /home/jovyan/work is on sys.path so `pipeline` imports work under spark-submit.
sys.path.insert(0, "/home/jovyan/work")

from pyspark.sql import SparkSession  # noqa: E402

from pipeline import config  # noqa: E402
from pipeline.etl import run_etl  # noqa: E402


def build_connections() -> dict:
    return {
        "paths": config.paths(),
        "postgres": config.postgres_kwargs(),
        "postgres_jdbc": config.postgres_jdbc(),
        "redis": config.redis_kwargs(),
    }


def main() -> None:
    spark = (SparkSession.builder
        .appName("run-etl")
        .getOrCreate())

    landing = os.environ.get("LANDING_DIR", config.paths().landing)
    run_etl(spark, landing, build_connections())

    spark.stop()


if __name__ == "__main__":
    main()
