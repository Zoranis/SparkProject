"""In-container post-up setup: jupytext conversion only.

Directory creation + sandbox seed now live in `init_data.py` (they happen
together with the download/split into the `spark_data` named volume).
This script stays small so `init.sh` has a single place to run "things that
depend on the `/home/jovyan/work` bind mounts being live" (i.e. notebooks/).
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

NOTEBOOKS = Path("/home/jovyan/work/notebooks")


def convert_notebooks() -> None:
    if not NOTEBOOKS.exists():
        return
    for md in NOTEBOOKS.glob("*.md"):
        ipynb = md.with_suffix(".ipynb")
        if ipynb.exists() and ipynb.stat().st_mtime >= md.stat().st_mtime:
            continue
        print(f"[post_up] jupytext: {md.name} → {ipynb.name}")
        subprocess.run(
            ["/opt/conda/bin/jupytext", "--quiet", "--to", "ipynb", str(md)],
            check=True,
        )


def main() -> None:
    convert_notebooks()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[post_up] ERROR: {e}", file=sys.stderr)
        sys.exit(1)
