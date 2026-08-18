# project-august-02-etl-pipeline-airflow

Airflow-orchestrated ETL pipeline that ingests GitHub repository commit
activity, cleans and validates it with pandas, and loads it idempotently
into a SQLite warehouse table.

## Problem statement

Teams that want visibility into repository activity (commit velocity, who's
committing, how many merges vs. direct commits) need a repeatable pipeline
that pulls raw activity data on a schedule, guarantees it's clean before it
lands anywhere, and can be re-run safely without producing duplicate or
stale rows. This project builds that pipeline end to end:

1. **Extract** — pull commit records from the public GitHub REST API
   (`GET /repos/{owner}/{repo}/commits`), which requires no API key for
   public repositories and updates continuously as new commits land, making
   it a genuine "real-time" source rather than a static snapshot.
2. **Transform** — flatten and clean the nested JSON into a tidy table with
   pandas: parse timestamps, normalize missing values, flag merge commits,
   deduplicate.
3. **Validate** — run a data-quality gate (schema conformance, required
   columns, null checks on primary/foreign fields, primary-key uniqueness)
   before anything is allowed to reach the warehouse.
4. **Load** — upsert into a SQLite table keyed by commit `sha`, so re-running
   the pipeline (a scheduler retry, a backfill, a manual trigger) never
   creates duplicates and always reflects the latest data.

## Architecture & design decisions

```
dags/github_commits_etl_dag.py   Airflow DAG (TaskFlow API): extract >> transform >> validate >> load
src/github_etl/
  config.py     Env-var-driven runtime configuration (repo, timeouts, retries, warehouse path)
  extract.py    GitHub API client: pagination, retry/backoff on 429/5xx, raises on 4xx
  transform.py  Raw JSON -> clean pandas DataFrame (flatten, parse, dedupe, sort)
  validate.py   Data-quality gate: schema, dtypes, not-null, uniqueness checks
  load.py       Idempotent SQLite upsert (INSERT ... ON CONFLICT(sha) DO UPDATE)
  serde.py      Round-trips a DataFrame through JSON-safe records for Airflow XCom
  pipeline.py   Airflow-agnostic orchestration function (used by both the DAG and a plain CLI)
tests/
  test_extract.py         HTTP layer, mocked with `responses` — no network in tests
  test_transform.py       Cleaning/dedup/sort logic
  test_validate.py        Every data-quality rule, both pass and fail paths
  test_load.py            Insert, idempotent re-run, upsert-on-change, on-disk creation
  test_pipeline.py        Full extract->load flow against a mocked API
  test_dag_validation.py  Airflow's own DagBag: import errors, task graph, cycle check
```

Key decisions:

- **Each pipeline stage is a plain Python function**, not an Airflow
  operator subclass. `pipeline.py` calls them directly for local runs and
  tests; the DAG wraps the same functions in `@task`-decorated closures.
  This keeps the core logic testable without a running Airflow instance and
  is why `tests/` never needs to spin up a scheduler or webserver.
- **XCom payloads are plain JSON** (`serde.py`), not pickled DataFrames.
  Airflow's default XCom backend JSON-serializes task return values, and
  pandas `Timestamp`/`numpy` scalars aren't JSON-safe, so `serde.py`
  explicitly converts to/from native Python types at each task boundary.
- **Idempotency lives in the load stage**, not in extraction. The extractor
  always pulls the same bounded page range; `load_commits` upserts on the
  commit `sha` primary key, so replaying a DAG run (retry, backfill, manual
  re-trigger) is always safe and never duplicates rows.
- **Validation is a hard gate.** `validate_commits` returns a
  `ValidationReport`; the DAG's `validate` task raises `AirflowException` on
  any failed check, which stops the `load` task from ever running against
  bad data (Airflow won't run a downstream task if its upstream fails).
- **No live network calls in unit tests.** `test_extract.py` and
  `test_pipeline.py` mock HTTP with the `responses` library so the suite is
  fast, deterministic, and runs in CI without depending on GitHub's
  availability or rate limits. The pipeline was also run end-to-end against
  the live GitHub API during development to confirm it genuinely works (see
  Example output below).
- **DAG-level tests use Airflow's own `DagBag`** loader plus
  `airflow.utils.dag_cycle_tester.check_cycle`, catching import errors,
  wrong task wiring, and cycles in the actual file Airflow parses — a class
  of bug unit tests on `github_etl` alone can't catch.
- **Data source is configurable** via `ETL_GITHUB_OWNER` / `ETL_GITHUB_REPO`
  (see Configuration below); it defaults to this repository itself so the
  pipeline is runnable out of the box with zero setup.

## Setup & run instructions

Requires Python 3.9+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

### Run the pipeline standalone (no Airflow needed)

```bash
python -m github_etl.pipeline
```

This extracts, transforms, validates, and loads commit activity for the
configured repository into `warehouse/github_activity.db`.

### Run it under Airflow

```bash
export AIRFLOW_HOME=~/airflow_demo
airflow db migrate
airflow dags test github_commits_etl 2026-08-18
```

`airflow dags test` runs every task in the DAG once, in order, without
needing the scheduler or webserver running — good for a quick local check.
For continuous operation, point `AIRFLOW__CORE__DAGS_FOLDER` at this repo's
`dags/` directory and start the scheduler and webserver normally; the DAG is
scheduled `@hourly` with `catchup=False`.

### Configuration

All configuration is via environment variables (see `src/github_etl/config.py`):

| Variable                  | Default                                            | Purpose                                |
|----------------------------|-----------------------------------------------------|-----------------------------------------|
| `ETL_GITHUB_OWNER`         | `shantanu2006-football`                             | Repository owner to ingest commits from |
| `ETL_GITHUB_REPO`          | `project-august-02-etl-pipeline-airflow`             | Repository name                         |
| `ETL_GITHUB_PER_PAGE`      | `100`                                                | Commits per API page                    |
| `ETL_GITHUB_MAX_PAGES`     | `5`                                                  | Max pages fetched per run (bounds a run)|
| `ETL_HTTP_TIMEOUT`         | `10`                                                 | Per-request timeout (seconds)           |
| `ETL_HTTP_MAX_RETRIES`     | `3`                                                  | Retries on 429/5xx                      |
| `ETL_WAREHOUSE_PATH`       | `warehouse/github_activity.db`                       | SQLite file path                        |
| `ETL_WAREHOUSE_TABLE`      | `commits`                                            | Target table name                       |
| `GITHUB_TOKEN`             | unset                                                | Optional; raises GitHub's rate limit    |

### Run the tests

```bash
pytest -v
```

37 tests, all offline (HTTP mocked), covering extract retry/pagination
logic, transform cleaning/dedup, every validation rule, idempotent load
behavior, the full pipeline, and DAG structure validation.

## Example output

A real run against this repository's live commit history
(`python -m github_etl.pipeline`):

```
2026-08-18 06:17:24 INFO github_etl.extract: Fetched 1 commit records for shantanu2006-football/project-august-02-etl-pipeline-airflow
2026-08-18 06:17:24 INFO github_etl.transform: Transformed 1 raw record(s) into 1 clean row(s)
2026-08-18 06:17:24 INFO github_etl.validate: Data quality checks passed (17 checks, 1 rows)
2026-08-18 06:17:24 INFO github_etl.load: Upserted 1 row(s) into 'commits'
PipelineResult(extracted_count=1, transformed_count=1, loaded_count=1, validation_passed=True, ...)
```

Resulting warehouse row:

```
sha                                       author_name        is_merge_commit   message_subject
75758535d2a0f33aad83929ff7d52f99c7fc078c  shantanu puthran   0                 Initial commit
```

## Future work

Cut from this iteration to keep scope tight for a single session:

- **Postgres backend** — `load.py` currently targets SQLite only; swapping
  in Postgres would mean replacing the raw `sqlite3` upsert with SQLAlchemy
  Core and a `postgresql://` connection string, gated behind the same
  `PipelineConfig`.
- **Incremental extraction** — the extractor re-fetches the same bounded
  page range every run rather than tracking a `since` cursor; correctness
  today comes entirely from the idempotent upsert, which is safe but
  re-reads more than strictly necessary as history grows.
- **Additional GitHub entities** — issues, pull requests, and releases share
  the same shape of problem and could reuse `validate.py`/`load.py` with a
  new extractor and transform.
- **Alerting on data-quality failures** — today a failed `validate` task
  just fails the DAG run; wiring Airflow's `on_failure_callback` to Slack/email
  would close the loop for an on-call team.
