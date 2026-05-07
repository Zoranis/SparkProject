# Spark Lab — Introduction

## Getting Started

```bash
make lab-spark          # bring up the cluster; bootstraps TLC data into the spark_data volume
                        #   (download + daily-split + sandbox seed — inside spark-jupyter)
make spark-producer-start   # start the file-drop producer (inside spark-jupyter)
make spark-producer-status  # peek at bucket counts and producer pid
make spark-producer-stop    # stop the producer
make spark-producer-reset   # clear landing/in_process/archive/errors between runs
make test-spark         # run the stage tests
```

Once `lab-spark` is up:

| URL | What |
|---|---|
| <http://localhost:8888> | JupyterLab — open the exploration notebook here |
| <http://localhost:4040> | Spark Driver UI (only while a job runs) |
| <http://localhost:8080> | Spark Master UI |

---

## The Business

You've been dropped into an NYC taxi platform's data team. Trips happen, files
land, consumers need answers. You're going to build the pipeline in the middle.

Two consumers will call into your code, and they want very different things:

- **The analyst** asks `total_revenue(d, h)` — revenue in a given hour of a given
  day, across all zones. They're fine with a ~1 second response.
- **The backend service** asks `avg_revenue(h)` — the average hourly revenue for
  a given hour-of-day, across the whole history. Millisecond budget. This call
  is on a user-facing path.

Between them sits a **producer** (controlled by `make spark-producer-start` /
`spark-producer-stop` / `spark-producer-status` / `spark-producer-reset`) that drops one new
daily parquet file into a landing directory every 10 seconds. Your job is the
pipeline from the landing directory through to the two consumer calls.

---

## Why You Are Here

This is not a SQL exercise. The SQL you'll write is small. The interesting
decisions are:

- A file has just landed. How do you know whether you've already processed it?
- Your job ran twice because the scheduler retried. What does that do to your
  aggregates?
- The analyst store is fine for 1-second queries. It is *not* fine for the
  millisecond-budget consumer. Where does that call actually read from?
- What keeps two copies of the ETL from stomping on each other?

Some of these questions have multiple reasonable answers, and we'll pick
between them in class. That's why the stage lessons describe the **problem**
and the **contract** — not the implementation.

---

## How the Project Works

You implement four functions across three files:

```python
# pipeline/migrate.py
def migrate(pg_kwargs: dict) -> None: ...           # idempotent schema prep

# pipeline/etl.py
def run_etl(spark_session, input_file_dir, connections): ...

# pipeline/serving.py
def total_revenue(d: str, h: int) -> float: ...     # ≤ 1s
def avg_revenue(h: int) -> float: ...               # ≤ 50ms median
```

- `run_etl` is called once per scheduler tick (every minute) by
  `pipeline/scheduler.py`, via `spark-submit` against the standalone cluster.
- `total_revenue` and `avg_revenue` are called directly by the tests, and in a
  real system would be called by the analyst's query layer and the backend API.

`connections` is a plain `dict`. Its shape is decided in class once the storage
technologies are chosen — tests and the scheduler build the same dict so your
code gets exactly one authority on what's inside it.

---

## Project Structure

```
.                                     ← project root
├── 00-introduction.md                ← you are here
├── stages/                           ← stage-by-stage lessons
│   ├── 01-exploration/
│   ├── 02-incremental-ingest/
│   ├── 03-etl-analyst-store/
│   ├── 04-backend-serving/
│   └── 05-bonus/
├── notebooks/
│   └── 00-exploration.ipynb          ← stage 1 only; the rest is .py
├── pipeline/
│   ├── migrate.py                    ← YOU MODIFY — migrate
│   ├── etl.py                        ← YOU MODIFY — run_etl
│   ├── serving.py                    ← YOU MODIFY — total_revenue, avg_revenue
│   ├── config.py                     ← connections dict assembly (provided)
│   ├── reset.py                      ← wipe helper used by tests (provided)
│   └── scheduler.py                  ← minute-cadence scheduler (provided)
├── jobs/run_etl_job.py               ← spark-submit entrypoint (provided)
├── tests/                            ← stage tests (provided, don't edit)
└── scripts/producer.py               ← the file-dropping producer (provided)
```

You modify three files: `pipeline/migrate.py`, `pipeline/etl.py`, and
`pipeline/serving.py`. Everything else is infrastructure.

---

## Workflow

1. **Explore** — run `notebooks/00-exploration.ipynb` against `data/sandbox/`
   (10 pre-seeded daily files). Understand the shape, the volume, the grain.
2. **Read the stage** — open `stages/0N-.../lesson.md` for the contract and
   acceptance criteria.
3. **Implement** — edit `pipeline/etl.py` and/or `pipeline/serving.py`.
4. **Test** — `make test-spark` (runs inside `spark-jupyter`).
5. **Run for real** — `python -m pipeline.scheduler` from inside `spark-jupyter`,
   watch the producer, watch the analyst store grow.

---

## Test Commands

| Command | Runs |
|---|---|
| `make test-spark` | all stage tests |
| `docker exec spark-jupyter pytest tests/test_stage2_incrementality.py -v` | stage 2 only |
| `docker exec spark-jupyter pytest tests/test_stage3_idempotency.py -v` | stage 3 only |
| `docker exec spark-jupyter pytest tests/test_stage4_latency.py -v` | stage 4 only |

Tests call your three functions as a black box. They do not import anything
from `pipeline/` besides `etl.run_etl`, `serving.total_revenue`, and
`serving.avg_revenue`.

---

## Submission

This project is scoped across multiple sessions. You'll start it in class and
finish it individually. Submit:

- Your `pipeline/` directory.
- A short `DESIGN.md` covering the decisions you made and why (especially the
  storage choices and your answer to the incrementality/idempotency question).
- `make test-spark` passing against your code.
