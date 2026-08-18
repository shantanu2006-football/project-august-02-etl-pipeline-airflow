"""Data quality gate: schema conformance and null checks for the cleaned DataFrame.

This runs between transform and load. A failure here should stop the
pipeline before bad data ever reaches the warehouse.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from github_etl.transform import OUTPUT_COLUMNS

logger = logging.getLogger(__name__)

# Columns that must never be null — everything the warehouse's primary key
# and any downstream join/aggregation depends on.
NOT_NULL_COLUMNS = ["sha", "author_name", "author_email", "committed_at"]

EXPECTED_DTYPE_KINDS = {
    "sha": "O",
    "author_name": "O",
    "author_email": "O",
    "author_login": "O",
    "committed_at": "M",  # datetime64
    "message_subject": "O",
    "message": "O",
    "is_merge_commit": "b",
    "message_length": "i",
    "url": "O",
}


class DataQualityError(ValueError):
    """Raised when a DataFrame fails one or more data quality checks."""


@dataclass
class ValidationReport:
    row_count: int
    checks_passed: list[str] = field(default_factory=list)
    checks_failed: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.checks_failed

    def raise_if_invalid(self) -> None:
        if not self.is_valid:
            raise DataQualityError(
                f"Data quality checks failed ({len(self.checks_failed)} issue(s)): "
                + "; ".join(self.checks_failed)
            )


def _check_schema(df: pd.DataFrame, report: ValidationReport) -> None:
    missing = [c for c in OUTPUT_COLUMNS if c not in df.columns]
    if missing:
        report.checks_failed.append(f"missing required column(s): {missing}")
    else:
        report.checks_passed.append("all required columns present")

    unexpected = [c for c in df.columns if c not in OUTPUT_COLUMNS]
    if unexpected:
        report.checks_failed.append(f"unexpected column(s): {unexpected}")
    else:
        report.checks_passed.append("no unexpected columns")


def _check_dtypes(df: pd.DataFrame, report: ValidationReport) -> None:
    for column, expected_kind in EXPECTED_DTYPE_KINDS.items():
        if column not in df.columns:
            continue
        actual_kind = df[column].dtype.kind
        if actual_kind != expected_kind:
            report.checks_failed.append(
                f"column '{column}' has dtype kind '{actual_kind}', expected '{expected_kind}'"
            )
        else:
            report.checks_passed.append(f"column '{column}' has expected dtype")


def _check_not_null(df: pd.DataFrame, report: ValidationReport) -> None:
    for column in NOT_NULL_COLUMNS:
        if column not in df.columns:
            continue
        null_count = int(df[column].isna().sum())
        if null_count:
            report.checks_failed.append(f"column '{column}' has {null_count} null value(s)")
        else:
            report.checks_passed.append(f"column '{column}' has no nulls")


def _check_unique_sha(df: pd.DataFrame, report: ValidationReport) -> None:
    if "sha" not in df.columns:
        return
    dupe_count = int(df["sha"].duplicated().sum())
    if dupe_count:
        report.checks_failed.append(f"'sha' column has {dupe_count} duplicate value(s)")
    else:
        report.checks_passed.append("'sha' column is unique")


def validate_commits(df: pd.DataFrame) -> ValidationReport:
    """Run the full data quality suite against a transformed commits DataFrame.

    Returns a :class:`ValidationReport`; callers that want fail-fast behaviour
    should call ``report.raise_if_invalid()``.
    """
    report = ValidationReport(row_count=len(df))

    if df.empty:
        report.checks_failed.append("DataFrame is empty — nothing to load")
        return report

    _check_schema(df, report)
    _check_dtypes(df, report)
    _check_not_null(df, report)
    _check_unique_sha(df, report)

    if report.checks_failed:
        logger.error("Data quality checks failed: %s", report.checks_failed)
    else:
        logger.info(
            "Data quality checks passed (%d checks, %d rows)",
            len(report.checks_passed),
            report.row_count,
        )
    return report
