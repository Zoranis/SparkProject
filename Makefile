# ─── Spark ETL scaffold ─────────────────────────────────────────────────────
#
# Run these targets from the scaffold root (the directory that holds this
# Makefile and compose.yml). The scaffold is self-contained: it brings up its
# own standalone Spark cluster + Postgres + Redis + Jupyter driver and does
# not merge with labs/base/compose.yml.
# ────────────────────────────────────────────────────────────────────────────

.PHONY: lab-spark lab-spark-down data-spark test-spark spark-migrate \
        spark-producer-start spark-producer-stop spark-producer-status spark-producer-reset \
        help

help:
	@echo ""
	@echo "  make lab-spark              Start Spark standalone cluster + Postgres + Redis + Jupyter"
	@echo "  make lab-spark-down         Stop the Spark lab"
	@echo "  make data-spark             Re-run the in-container data bootstrap (idempotent)"
	@echo "  make test-spark             Run the stage pytest suite inside spark-jupyter"
	@echo "  make spark-migrate          Run pipeline.migrate() against spark-postgres (idempotent)"
	@echo "  make spark-producer-start   Start the file-drop producer inside spark-jupyter"
	@echo "  make spark-producer-stop    Stop the running producer"
	@echo "  make spark-producer-status  Show producer + bucket state"
	@echo "  make spark-producer-reset   Clear landing/in_process/archive/errors buckets"
	@echo ""

lab-spark: lab-spark-down
	docker compose up -d --build --remove-orphans --wait
	@echo ""
	@echo "=============================================================="
	@echo " Spark lab is ready."
	@echo "   Jupyter (driver):   http://localhost:8888"
	@echo "   Spark driver UI:    http://localhost:4040  (after a job runs)"
	@echo "   Spark master UI:    http://localhost:8080"
	@echo "   Postgres:           localhost:5432  (user=spark pass=spark db=taxi)"
	@echo "   Redis:              localhost:6379"
	@echo "   Producer:           make spark-producer-start / -status / -stop / -reset"
	@echo "=============================================================="

lab-spark-down:
	docker compose down --remove-orphans

data-spark:
	docker exec spark-jupyter python /home/jovyan/work/scripts/init_data.py

test-spark:
	docker exec spark-jupyter pytest /home/jovyan/work/tests -v

spark-migrate:
	docker exec spark-jupyter python -c "from pipeline import config; from pipeline.migrate import migrate; migrate(config.postgres_kwargs()); print('[migrate] done')"

spark-producer-start:
	docker exec spark-jupyter python /home/jovyan/work/scripts/producer.py start

spark-producer-stop:
	docker exec spark-jupyter python /home/jovyan/work/scripts/producer.py stop

spark-producer-status:
	docker exec spark-jupyter python /home/jovyan/work/scripts/producer.py status

spark-producer-reset:
	docker exec spark-jupyter python /home/jovyan/work/scripts/producer.py reset
