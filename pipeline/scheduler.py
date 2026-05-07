"""Instructor-owned scheduler.

Every TICK_SECONDS (default 60), spark-submit the ETL job against the standalone
cluster. A lockfile enforces single-writer: if the previous submit is still
running when a tick fires, this tick is skipped rather than stacked.

This is the production-shape execution mode — a fresh SparkContext per tick,
not a long-running driver. JVM cold-start (~5-10s) is acceptable inside a 60s
window, and it forces the ETL to be idempotent + incremental rather than relying
on in-memory state between runs.

Run inside spark-jupyter:

    python -m pipeline.scheduler
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

TICK_SECONDS = int(os.environ.get("SCHEDULER_TICK_SECONDS", "60"))
LOCK_PATH = Path(os.environ.get("SCHEDULER_LOCK", "/tmp/run_etl.lock"))
JOB_PATH = os.environ.get("ETL_JOB", "/home/jovyan/work/jobs/run_etl_job.py")
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "/usr/local/spark/bin/spark-submit")


def _holder_alive() -> bool:
    if not LOCK_PATH.exists():
        return False
    try:
        pid = int(LOCK_PATH.read_text().strip())
    except ValueError:
        LOCK_PATH.unlink(missing_ok=True)
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        LOCK_PATH.unlink(missing_ok=True)
        return False


def _tick() -> None:
    if _holder_alive():
        print(f"[scheduler] previous submit still running (pid {LOCK_PATH.read_text().strip()}); skipping tick")
        return
    proc = subprocess.Popen(
        [SPARK_SUBMIT, "--master", SPARK_MASTER, JOB_PATH],
        stdout=sys.stdout, stderr=sys.stderr,
    )
    LOCK_PATH.write_text(str(proc.pid))
    rc = proc.wait()
    LOCK_PATH.unlink(missing_ok=True)
    print(f"[scheduler] tick done (rc={rc})")


def main() -> None:
    stop = False

    def _graceful(_sig, _frame):
        nonlocal stop
        stop = True
        print("[scheduler] stop requested; will exit after current tick")

    signal.signal(signal.SIGINT, _graceful)
    signal.signal(signal.SIGTERM, _graceful)

    print(f"[scheduler] cadence={TICK_SECONDS}s job={JOB_PATH} master={SPARK_MASTER}")
    next_tick = time.monotonic()
    while not stop:
        _tick()
        next_tick += TICK_SECONDS
        delay = max(0.0, next_tick - time.monotonic())
        time.sleep(delay)


if __name__ == "__main__":
    main()
