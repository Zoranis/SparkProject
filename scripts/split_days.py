"""Split monthly NYC TLC parquet files into per-day parquet files, using DuckDB.

Runs inside `spark-jupyter`. Called by `init_data.py` after the raw monthly
files have been downloaded into the `spark_data` volume.

Output layout: one file per day, named `YYYY-MM-DD.parquet`, under the given
`days_dir`. Flat file names (no Hive-style partitioning) — the producer and
the stage tests expect `2019-MM-DD.parquet`.

Idempotent: if a given day's file already exists, it's not rewritten. If all
of a month's days are already present, the monthly file is skipped entirely.

Single-pass strategy: one `COPY ... PARTITION_BY (d)` per month. DuckDB reads
the monthly parquet once and writes all ~30 daily files concurrently into a
Hive-style staging dir (`d=YYYY-MM-DD/data_0.parquet`), which we then rename
to flat `YYYY-MM-DD.parquet`. This is ~30× faster than issuing one `COPY` per
day (which re-reads the monthly parquet each time).

Usage:
    python -m scripts.split_days <raw_dir> <days_dir>
"""
from __future__ import annotations

import re
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb

MONTH_COMPLETE_THRESHOLD = 28


@contextmanager
def timed(label: str):
    t0 = time.perf_counter()
    yield
    print(f"  [timing] {label}: {time.perf_counter() - t0:.2f}s", flush=True)


def _harvest_hive_output(stage: Path, days_dir: Path, month: str) -> int:
    """Move `stage/d=YYYY-MM-DD/data_0.parquet` → `days_dir/YYYY-MM-DD.parquet`.

    Skips any day-file that already exists (idempotency).
    """
    written = 0
    for part_dir in sorted(stage.glob("d=2019-*")):
        d = part_dir.name.removeprefix("d=")
        if not d.startswith(f"2019-{month}-"):
            continue
        out = days_dir / f"{d}.parquet"
        if out.exists():
            continue
        files = list(part_dir.glob("*.parquet"))
        if not files:
            continue
        if len(files) == 1:
            shutil.move(str(files[0]), str(out))
        else:
            # DuckDB may split a partition into multiple files on large days;
            # merge them by concat-via-duckdb. Rare for NYC TLC daily volumes.
            con = duckdb.connect()
            try:
                out_str = str(out).replace("'", "''")
                srcs = ",".join(f"'{str(f).replace(chr(39), chr(39) * 2)}'" for f in files)
                con.execute(
                    f"COPY (SELECT * FROM read_parquet([{srcs}])) "
                    f"TO '{out_str}' (FORMAT PARQUET)"
                )
            finally:
                con.close()
        written += 1
    return written


def split_month(con: duckdb.DuckDBPyConnection, monthly: Path, days_dir: Path) -> float:
    t_month = time.perf_counter()
    m = re.search(r"2019-(\d{2})", monthly.name)
    if not m:
        return 0.0
    month = m.group(1)
    prefix = f"2019-{month}-"

    existing = {p.stem for p in days_dir.glob(f"{prefix}*.parquet")}
    if len(existing) >= MONTH_COMPLETE_THRESHOLD:
        print(f"  - {monthly.name}: {len(existing)} days already present, skipping", flush=True)
        return 0.0

    size_mb = monthly.stat().st_size // 1_000_000
    print(f"  - reading {monthly.name} ({size_mb} MB)", flush=True)
    src = str(monthly).replace("'", "''")

    with tempfile.TemporaryDirectory(prefix=f".split_{month}_", dir=str(days_dir)) as tmp:
        stage = Path(tmp)
        stage_str = str(stage).replace("'", "''")
        with timed(f"partitioned write for 2019-{month}"):
            con.execute(
                f"""
                COPY (
                    SELECT *, CAST(tpep_pickup_datetime AS DATE) AS d
                    FROM read_parquet('{src}')
                    WHERE tpep_pickup_datetime IS NOT NULL
                      AND strftime(tpep_pickup_datetime, '%Y-%m') = '2019-{month}'
                )
                TO '{stage_str}'
                (FORMAT PARQUET, PARTITION_BY (d), OVERWRITE_OR_IGNORE)
                """
            )
        with timed(f"rename to flat names for 2019-{month}"):
            written = _harvest_hive_output(stage, days_dir, month)

    dt_month = time.perf_counter() - t_month
    per_day = (dt_month / written) if written else 0.0
    print(
        f"  [timing] 2019-{month} TOTAL: {dt_month:.2f}s "
        f"({written} daily files, {per_day:.2f}s/day)",
        flush=True,
    )
    return dt_month


def main(raw_dir: Path, days_dir: Path) -> None:
    days_dir.mkdir(parents=True, exist_ok=True)
    t_all = time.perf_counter()
    con = duckdb.connect()
    try:
        per_month = []
        for monthly in sorted(raw_dir.glob("yellow_tripdata_2019-*.parquet")):
            per_month.append((monthly.name, split_month(con, monthly, days_dir)))
        total = len(list(days_dir.glob("2019-*.parquet")))
        dt_all = time.perf_counter() - t_all
        avg_per_day = (dt_all / total) if total else 0.0
        print(f"[split_days] total daily files: {total}", flush=True)
        print(
            f"[split_days] TOTAL wall time: {dt_all:.2f}s "
            f"({avg_per_day:.2f}s/day across all months)",
            flush=True,
        )
        nonzero = [t for _, t in per_month if t > 0]
        if nonzero:
            print(
                f"[split_days] per-month (worked): min={min(nonzero):.2f}s "
                f"max={max(nonzero):.2f}s avg={sum(nonzero) / len(nonzero):.2f}s",
                flush=True,
            )
    finally:
        con.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.stderr.write("usage: split_days.py <raw_dir> <days_dir>\n")
        sys.exit(2)
    main(Path(sys.argv[1]), Path(sys.argv[2]))
