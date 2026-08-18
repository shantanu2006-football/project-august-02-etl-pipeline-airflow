"""Load the cleaned commits DataFrame into a SQLite warehouse table, idempotently.

Idempotency strategy: ``sha`` is the primary key, and loads use
``INSERT ... ON CONFLICT(sha) DO UPDATE`` (SQLite upsert, available since
3.24). Re-running the pipeline against the same source data is therefore
safe — existing rows are refreshed in place and no duplicates are created.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

import pandas as pd

from github_etl.config import DEFAULT_CONFIG, PipelineConfig

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    sha              TEXT PRIMARY KEY,
    author_name      TEXT NOT NULL,
    author_email     TEXT NOT NULL,
    author_login     TEXT,
    committed_at     TEXT NOT NULL,
    message_subject  TEXT NOT NULL,
    message          TEXT NOT NULL,
    is_merge_commit  INTEGER NOT NULL,
    message_length   INTEGER NOT NULL,
    url              TEXT,
    loaded_at        TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'))
)
"""

_UPSERT_SQL = """
INSERT INTO {table} (
    sha, author_name, author_email, author_login, committed_at,
    message_subject, message, is_merge_commit, message_length, url
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(sha) DO UPDATE SET
    author_name = excluded.author_name,
    author_email = excluded.author_email,
    author_login = excluded.author_login,
    committed_at = excluded.committed_at,
    message_subject = excluded.message_subject,
    message = excluded.message,
    is_merge_commit = excluded.is_merge_commit,
    message_length = excluded.message_length,
    url = excluded.url
"""


def get_connection(warehouse_path: str) -> sqlite3.Connection:
    path = Path(warehouse_path)
    if str(path.parent) not in ("", "."):
        os.makedirs(path.parent, exist_ok=True)
    return sqlite3.connect(warehouse_path)


def _row_to_params(row: pd.Series) -> tuple:
    committed_at = row["committed_at"]
    committed_at_str = committed_at.isoformat() if pd.notna(committed_at) else None
    return (
        row["sha"],
        row["author_name"],
        row["author_email"],
        row["author_login"] if pd.notna(row["author_login"]) else None,
        committed_at_str,
        row["message_subject"],
        row["message"],
        int(bool(row["is_merge_commit"])),
        int(row["message_length"]),
        row["url"] if pd.notna(row["url"]) else None,
    )


def load_commits(
    df: pd.DataFrame,
    config: PipelineConfig = DEFAULT_CONFIG,
    connection: sqlite3.Connection | None = None,
) -> int:
    """Upsert every row of ``df`` into the warehouse table.

    Returns the number of rows written. Pass an explicit ``connection`` to
    reuse an open connection (e.g. an in-memory database in tests); otherwise
    a new connection to ``config.warehouse_path`` is opened and closed here.
    """
    if df.empty:
        logger.info("No rows to load; skipping")
        return 0

    owns_connection = connection is None
    conn = connection or get_connection(config.warehouse_path)
    table = config.warehouse_table
    try:
        conn.execute(_CREATE_TABLE_SQL.format(table=table))
        params = [_row_to_params(row) for _, row in df.iterrows()]
        conn.executemany(_UPSERT_SQL.format(table=table), params)
        conn.commit()
    finally:
        if owns_connection:
            conn.close()

    logger.info("Upserted %d row(s) into '%s'", len(params), table)
    return len(params)
