"""In-container data bootstrap. Runs inside `spark-jupyter`.

Everything data-related happens in the `spark_data` named volume — nothing
touches the student's host disk. Steps:

  1. Download the 12 monthly TLC Yellow Taxi 2019 parquet files into
     `/home/jovyan/work/data/raw/` (idempotent; skip per-file if present).
  2. Download the taxi zone lookup CSV (idempotent).
  3. Split each monthly file into per-day parquet files under
     `/home/jovyan/work/data/days/YYYY-MM-DD.parquet` via `split_days.main`.
  4. Seed `/home/jovyan/work/data/sandbox/` with the first 10 day-files so
     the exploration notebook has something to read without the producer.
  5. Ensure the bucket directories exist.
  6. Write a `.populated` sentinel so subsequent runs are nearly no-ops.

Skip shortcut: if `.populated` exists AND `days/` has >=360 files AND the
sandbox is non-empty, the script exits quickly.
"""
from __future__ import annotations

import shutil
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from split_days import main as split_main  # noqa: E402

DATA = Path("/home/jovyan/work/data")
RAW = DATA / "raw"
DAYS = DATA / "days"
SANDBOX = DATA / "sandbox"
SENTINEL = DATA / ".populated"
BUCKETS = ("landing", "in_process", "archive", "errors", "sandbox")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
MONTHS = [f"{m:02d}" for m in range(1, 13)]
SANDBOX_SIZE = 10


def _download(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        print(f"  - {dest.name} cached ({dest.stat().st_size // 1_000_000} MB)")
        return
    print(f"  - downloading {dest.name} ...")
    tmp = dest.with_suffix(dest.suffix + ".part")
    with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:
        shutil.copyfileobj(resp, out, length=1 << 20)
    tmp.rename(dest)
    print(f"    done ({dest.stat().st_size // 1_000_000} MB)")


def ensure_buckets() -> None:
    for name in BUCKETS:
        (DATA / name).mkdir(parents=True, exist_ok=True)


def download_raw() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    for m in MONTHS:
        name = f"yellow_tripdata_2019-{m}.parquet"
        _download(f"{BASE_URL}/trip-data/{name}", RAW / name)
    _download(f"{BASE_URL}/misc/taxi_zone_lookup.csv", DATA / "zones.csv")


def seed_sandbox() -> None:
    if any(SANDBOX.iterdir()):
        print(f"[init-data] sandbox already has {sum(1 for _ in SANDBOX.iterdir())} files; skipping")
        return
    sources = sorted(DAYS.glob("2019-*.parquet"))[:SANDBOX_SIZE]
    for src in sources:
        shutil.copy2(src, SANDBOX / src.name)
    print(f"[init-data] seeded sandbox with {len(sources)} files")


def already_done() -> bool:
    return (
        SENTINEL.exists()
        and DAYS.exists()
        and len(list(DAYS.glob("2019-*.parquet"))) >= 360
        and SANDBOX.exists()
        and any(SANDBOX.iterdir())
    )


def main() -> None:
    ensure_buckets()
    if already_done():
        print("[init-data] already populated; skipping")
        return
    t_all = time.perf_counter()

    print("[init-data] downloading raw monthly parquet + zones.csv ...", flush=True)
    t = time.perf_counter()
    download_raw()
    print(f"[init-data] [timing] download: {time.perf_counter() - t:.2f}s", flush=True)

    print("[init-data] splitting monthly → daily parquet under data/days/ ...", flush=True)
    t = time.perf_counter()
    split_main(RAW, DAYS)
    print(f"[init-data] [timing] split: {time.perf_counter() - t:.2f}s", flush=True)

    print("[init-data] seeding sandbox ...", flush=True)
    t = time.perf_counter()
    seed_sandbox()
    print(f"[init-data] [timing] seed sandbox: {time.perf_counter() - t:.2f}s", flush=True)

    SENTINEL.touch()
    print(f"[init-data] done. TOTAL: {time.perf_counter() - t_all:.2f}s", flush=True)


if __name__ == "__main__":
    main()
