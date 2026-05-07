#!/usr/bin/env python3
"""Post-init hook for the spark-etl scaffold.

Invoked by scripts/new-project.py after the scaffold has been copied to the
student's target directory. CWD is the target. Stdlib-only — students have
not run `uv sync` yet.

Responsibilities:
  * Ensure data/ exists with a .gitkeep so git tracks the empty bucket tree
    the producer/ETL will later populate.
"""
from __future__ import annotations

import sys
from pathlib import Path


def main(target: Path) -> int:
    data = target / "data"
    data.mkdir(exist_ok=True)
    (data / ".gitkeep").touch(exist_ok=True)
    print(f"[post-init] ensured {data}/.gitkeep")
    return 0


if __name__ == "__main__":
    target = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path.cwd()
    sys.exit(main(target))
