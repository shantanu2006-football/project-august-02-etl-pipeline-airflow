"""End-to-end orchestration: extract -> transform -> validate -> load.

This module is intentionally Airflow-agnostic — each stage is a plain
function that the Airflow DAG (``dags/github_commits_etl_dag.py``) wires up
as tasks via XCom, and that a CLI or test can call directly without spinning
up a scheduler.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd

from github_etl.config import DEFAULT_CONFIG, PipelineConfig
from github_etl.extract import fetch_commits
from github_etl.load import load_commits
from github_etl.transform import transform_commits
from github_etl.validate import validate_commits

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    extracted_count: int
    transformed_count: int
    loaded_count: int
    validation_passed: bool
    validation_messages: list[str]


def run_pipeline(config: PipelineConfig = DEFAULT_CONFIG) -> PipelineResult:
    """Run the full ETL pipeline once and return a summary of what happened."""
    raw_records: list[dict[str, Any]] = fetch_commits(config)
    df: pd.DataFrame = transform_commits(raw_records)

    if df.empty:
        logger.warning("No commit records to process after transform; skipping load")
        return PipelineResult(
            extracted_count=len(raw_records),
            transformed_count=0,
            loaded_count=0,
            validation_passed=True,
            validation_messages=["no rows to validate"],
        )

    report = validate_commits(df)
    report.raise_if_invalid()

    loaded_count = load_commits(df, config)

    return PipelineResult(
        extracted_count=len(raw_records),
        transformed_count=len(df),
        loaded_count=loaded_count,
        validation_passed=report.is_valid,
        validation_messages=report.checks_passed,
    )


if __name__ == "__main__":
    result = run_pipeline()
    logger.info("Pipeline finished: %s", result)
