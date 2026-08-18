"""Clean and normalize raw GitHub commit JSON into a tidy pandas DataFrame."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Canonical, ordered column set produced by this stage and expected downstream.
OUTPUT_COLUMNS = [
    "sha",
    "author_name",
    "author_email",
    "author_login",
    "committed_at",
    "message_subject",
    "message",
    "is_merge_commit",
    "message_length",
    "url",
]


def _safe_get(d: dict[str, Any] | None, *keys: str) -> Any:
    """Walk a chain of nested dict lookups, returning None on any missing/None link."""
    current: Any = d
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _record_to_row(record: dict[str, Any]) -> dict[str, Any]:
    message = _safe_get(record, "commit", "message") or ""
    message_subject = message.split("\n", 1)[0].strip()
    return {
        "sha": record.get("sha"),
        "author_name": _safe_get(record, "commit", "author", "name"),
        "author_email": _safe_get(record, "commit", "author", "email"),
        # The GitHub user account (nullable: commit authors need not have an account).
        "author_login": _safe_get(record, "author", "login"),
        "committed_at": _safe_get(record, "commit", "author", "date"),
        "message_subject": message_subject,
        "message": message,
        "is_merge_commit": message_subject.lower().startswith("merge"),
        "message_length": len(message),
        "url": record.get("html_url"),
    }


def transform_commits(raw_records: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert raw GitHub commit API records into a cleaned, deduplicated DataFrame.

    - Flattens the nested ``commit.author`` / ``commit.committer`` structures.
    - Parses ``committed_at`` into a timezone-aware UTC timestamp.
    - Drops rows with no ``sha`` (the primary key) since they cannot be loaded.
    - Deduplicates by ``sha``, keeping the first occurrence, so a source page
      returned twice (e.g. due to a retried request) does not create dupes.
    """
    if not raw_records:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    rows = [_record_to_row(record) for record in raw_records]
    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)

    before = len(df)
    df = df[df["sha"].notna() & (df["sha"].str.len() > 0)]
    dropped_no_sha = before - len(df)
    if dropped_no_sha:
        logger.warning("Dropped %d record(s) with missing sha", dropped_no_sha)

    before = len(df)
    df = df.drop_duplicates(subset="sha", keep="first")
    dropped_dupes = before - len(df)
    if dropped_dupes:
        logger.info("Dropped %d duplicate record(s) by sha", dropped_dupes)

    df["committed_at"] = pd.to_datetime(df["committed_at"], utc=True, errors="coerce")
    df["author_name"] = df["author_name"].fillna("unknown").astype(str)
    df["author_email"] = df["author_email"].fillna("unknown@example.com").astype(str)
    df["message_subject"] = df["message_subject"].fillna("").astype(str)
    df["message"] = df["message"].fillna("").astype(str)
    df["is_merge_commit"] = df["is_merge_commit"].astype(bool)
    df["message_length"] = df["message_length"].astype(int)

    df = df.sort_values("committed_at", ascending=False, na_position="last").reset_index(drop=True)

    logger.info("Transformed %d raw record(s) into %d clean row(s)", len(raw_records), len(df))
    return df
