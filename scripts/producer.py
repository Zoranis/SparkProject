"""Producer control plane — runs inside `spark-jupyter`.

Replaces the dedicated `spark-producer` container. Students use four commands:

    python scripts/producer.py start        # kick off a background loop
    python scripts/producer.py stop         # stop the background loop
    python scripts/producer.py status       # show state (pid, file counts)
    python scripts/producer.py reset        # clear landing/in_process/archive/errors

The producer drops one pre-built daily parquet from data/days/ into
data/landing/ every TICK_SECONDS (default 10). It writes to a `.inflight`
temp name first and renames, so a reader never sees a partial file. It
tracks progress via the filesystem itself — a file present in any of
landing/, in_process/, or archive/ is considered "already emitted."

State:
    /tmp/spark_producer.pid   PID of the running loop (absence = not running)
    /tmp/spark_producer.log   stdout/stderr of the background loop
"""
from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

DATA = Path(os.environ.get("SPARK_DATA_DIR", "/home/jovyan/work/data"))
DAYS = DATA / "days"
LANDING = DATA / "landing"
IN_PROCESS = DATA / "in_process"
ARCHIVE = DATA / "archive"
ERRORS = DATA / "errors"
BUCKETS = (LANDING, IN_PROCESS, ARCHIVE, ERRORS)

PID_FILE = Path("/tmp/spark_producer.pid")
LOG_FILE = Path("/tmp/spark_producer.log")
TICK_SECONDS = int(os.environ.get("TICK_SECONDS", "10"))


def _ensure_dirs() -> None:
    for b in BUCKETS:
        b.mkdir(parents=True, exist_ok=True)


def _running_pid() -> int | None:
    if not PID_FILE.exists():
        return None
    try:
        pid = int(PID_FILE.read_text().strip())
    except ValueError:
        PID_FILE.unlink(missing_ok=True)
        return None
    try:
        os.kill(pid, 0)
        return pid
    except ProcessLookupError:
        PID_FILE.unlink(missing_ok=True)
        return None


def _already_emitted(name: str) -> bool:
    return any((b / name).exists() for b in (LANDING, IN_PROCESS, ARCHIVE))


def _run_loop() -> None:
    """Foreground loop body. Called by `start --foreground` (which `start` spawns)."""
    _ensure_dirs()
    PID_FILE.write_text(str(os.getpid()))
    try:
        if not DAYS.exists() or not any(DAYS.iterdir()):
            print(f"[producer] {DAYS} empty — run 'make data-spark' first", flush=True)
            return
        sources = sorted(DAYS.glob("2019-*.parquet"))
        print(f"[producer] {len(sources)} day-files available, tick={TICK_SECONDS}s", flush=True)
        for src in sources:
            if _already_emitted(src.name):
                continue
            tmp = LANDING / f".{src.name}.inflight"
            shutil.copy2(src, tmp)
            tmp.rename(LANDING / src.name)
            print(f"[producer] dropped {src.name}", flush=True)
            time.sleep(TICK_SECONDS)
        print("[producer] all files emitted; exiting", flush=True)
    finally:
        PID_FILE.unlink(missing_ok=True)


def cmd_start(args: argparse.Namespace) -> int:
    if args.foreground:
        _run_loop()
        return 0
    pid = _running_pid()
    if pid is not None:
        print(f"[producer] already running (pid {pid})")
        return 0
    _ensure_dirs()
    # Ensure the analyst-store schema exists before we start dropping files.
    # migrate() is idempotent — safe to run on every start.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from pipeline import config
    from pipeline.migrate import migrate
    print("[producer] running migrate() ...")
    migrate(config.postgres_kwargs())
    log = open(LOG_FILE, "ab", buffering=0)
    proc = subprocess.Popen(
        [sys.executable, __file__, "start", "--foreground"],
        stdout=log, stderr=log, stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    # Give the child a moment to write its own pidfile; fall back to ours if needed.
    time.sleep(0.3)
    if not PID_FILE.exists():
        PID_FILE.write_text(str(proc.pid))
    print(f"[producer] started (pid {PID_FILE.read_text().strip()}) — log: {LOG_FILE}")
    return 0


def cmd_stop(_args: argparse.Namespace) -> int:
    pid = _running_pid()
    if pid is None:
        print("[producer] not running")
        return 0
    os.kill(pid, signal.SIGTERM)
    for _ in range(20):
        if _running_pid() is None:
            print(f"[producer] stopped (pid {pid})")
            return 0
        time.sleep(0.1)
    os.kill(pid, signal.SIGKILL)
    PID_FILE.unlink(missing_ok=True)
    print(f"[producer] force-killed (pid {pid})")
    return 0


def _count(p: Path) -> int:
    return sum(1 for _ in p.glob("*.parquet")) if p.exists() else 0


def cmd_status(_args: argparse.Namespace) -> int:
    pid = _running_pid()
    print(f"producer:    {'running (pid ' + str(pid) + ')' if pid else 'stopped'}")
    print(f"days/:       {_count(DAYS)} source files")
    print(f"landing/:    {_count(LANDING)} waiting")
    print(f"in_process/: {_count(IN_PROCESS)} mid-flight")
    print(f"archive/:    {_count(ARCHIVE)} consumed")
    print(f"errors/:     {_count(ERRORS)} failed")
    if LOG_FILE.exists():
        print(f"log:         {LOG_FILE} ({LOG_FILE.stat().st_size} bytes)")
    return 0


def cmd_reset(_args: argparse.Namespace) -> int:
    pid = _running_pid()
    if pid is not None:
        print(f"[producer] stop first (running, pid {pid})")
        return 1
    for b in BUCKETS:
        if b.exists():
            shutil.rmtree(b)
        b.mkdir(parents=True, exist_ok=True)
    LOG_FILE.unlink(missing_ok=True)
    print("[producer] reset: cleared landing/in_process/archive/errors and log")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Control the file-drop producer.")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("start", help="start the producer in the background")
    s.add_argument("--foreground", action="store_true", help=argparse.SUPPRESS)
    s.set_defaults(func=cmd_start)

    sub.add_parser("stop", help="stop the producer").set_defaults(func=cmd_stop)
    sub.add_parser("status", help="print producer and bucket state").set_defaults(func=cmd_status)
    sub.add_parser("reset", help="clear landing/in_process/archive/errors").set_defaults(func=cmd_reset)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
