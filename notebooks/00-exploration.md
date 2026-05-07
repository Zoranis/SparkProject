---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.19.1
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Stage 1 — Exploration

You have 10 pre-seeded daily parquet files under `/home/jovyan/work/data/sandbox/`. Use them to
answer the questions in `stages/01-exploration/lesson.md`. The live producer
is writing to `data/landing/` on a timer — **do not read from there yet**.

```{code-cell}
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("exploration")
    .getOrCreate())

trips = spark.read.parquet("/home/jovyan/work/data/sandbox/")
trips.createOrReplaceTempView("trips")

print("Driver UI:", spark.sparkContext.uiWebUrl)
trips.printSchema()
```

## Zone lookup (CSV)

The taxi-zone lookup ships as CSV next to the parquet. Load it with
`spark.read.csv` — `header=True` uses the first row as column names, and
`inferSchema=True` costs an extra pass but keeps the types honest.

```{code-cell}
zones = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/home/jovyan/work/data/zones.csv"))
zones.createOrReplaceTempView("zones")

zones.printSchema()
zones.show(5, truncate=False)
```

## How many rows per day?

```{code-cell}
(trips
    .selectExpr("CAST(tpep_pickup_datetime AS DATE) AS d")
    .groupBy("d")
    .count()
    .orderBy("d")
    .show(20, truncate=False))
```

## What columns do the consumer queries actually need?

The two consumer queries you'll build are:

- `total_revenue(d, h)` — sum of `total_amount` grouped by (day, hour).
- `avg_revenue(h)` — average hourly revenue grouped by hour-of-day.

Both need: `tpep_pickup_datetime`, `total_amount`. That's it.

```{code-cell}
spark.sql("""
    SELECT
        CAST(tpep_pickup_datetime AS DATE) AS d,
        HOUR(tpep_pickup_datetime)         AS h,
        ROUND(SUM(total_amount), 2)        AS revenue
    FROM trips
    GROUP BY CAST(tpep_pickup_datetime AS DATE), HOUR(tpep_pickup_datetime)
    ORDER BY d, h
""").show(24)
```

## Throughput math (do this in your head or on paper)

- 1 daily file every 10s → 6 files/minute → ~540 files/90-minute class.
- ~300k rows per file × 6 files/min → **~1.8M rows/minute** into landing.
- Analyst aggregate grain `(day, hour, zone)` caps out at 365 × 24 × 265 ≈
  **2.3M rows for the whole year**.

Write a one-sentence answer to each of the four questions in the stage lesson.
Take it to the class discussion.

```{code-cell}
spark.stop()
```
