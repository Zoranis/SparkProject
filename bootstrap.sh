#!/bin/bash
# Pre-launch hook for the Jupyter docker-stacks image.
# Ships in /usr/local/bin/before-notebook.d/ and runs as root (container starts
# with `user: root`; start-notebook.sh drops privileges to NB_USER afterwards).
# CHOWN_EXTRA has already fixed /home/jovyan/work/data ownership by the time
# we run, so init_data.py can mkdir the bucket subdirs as jovyan.
#
# Body runs in a subshell because docker-stacks sources *.sh hooks — without
# isolation, `set -e`/`set -u` would leak into start.sh and break it on the
# next unset variable (e.g. JUPYTER_ENV_VARS_TO_UNSET).
(
    set -euo pipefail

    echo "[bootstrap] populating spark_data volume (idempotent; first run ~5-10m) ..."
    su "${NB_USER}" -c "/opt/conda/bin/python /home/jovyan/work/scripts/init_data.py"

    echo "[bootstrap] converting notebooks (jupytext) ..."
    su "${NB_USER}" -c "/opt/conda/bin/python /home/jovyan/work/scripts/post_up.py"

    echo "[bootstrap] done."
)
