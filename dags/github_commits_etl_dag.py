"""Airflow DAG: ingest GitHub commit activity into a SQLite warehouse table.

Pipeline shape: extract -> transform -> validate -> load, one task per
stage, wired together with the TaskFlow API. Each task delegates to the
plain-Python functions in ``github_etl`` so the same logic is unit-testable
outside of Airflow (see ``tests/``) and re-runnable idempotently — the load
stage upserts on commit ``sha``, so replaying a DAG run never creates
duplicate warehouse rows.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from github_etl.config import DEFAULT_CONFIG
from github_etl.extract import fetch_commits
from github_etl.load import load_commits
from github_etl.serde import dataframe_to_records, records_to_dataframe
from github_etl.transform import transform_commits
from github_etl.validate import validate_commits

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="github_commits_etl",
    description="Extract GitHub commit activity, clean it, validate it, and load it into SQLite.",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["etl", "github", "sqlite"],
)
def github_commits_etl():
    @task
    def extract() -> list[dict]:
        return fetch_commits(DEFAULT_CONFIG)

    @task
    def transform(raw_records: list[dict]) -> list[dict]:
        df = transform_commits(raw_records)
        return dataframe_to_records(df)

    @task
    def validate(transformed_records: list[dict]) -> list[dict]:
        df = records_to_dataframe(transformed_records)
        if df.empty:
            return transformed_records
        report = validate_commits(df)
        if not report.is_valid:
            raise AirflowException(
                "Data quality checks failed: " + "; ".join(report.checks_failed)
            )
        return transformed_records

    @task
    def load(validated_records: list[dict]) -> int:
        df = records_to_dataframe(validated_records)
        return load_commits(df, DEFAULT_CONFIG)

    load(validate(transform(extract())))


github_commits_etl()
