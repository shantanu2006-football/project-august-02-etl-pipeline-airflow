from __future__ import annotations

import sqlite3

from github_etl.config import PipelineConfig
from github_etl.load import load_commits
from github_etl.transform import transform_commits

TEST_CONFIG = PipelineConfig(warehouse_table="commits")


def test_load_inserts_rows(raw_commit_records):
    conn = sqlite3.connect(":memory:")
    df = transform_commits(raw_commit_records)
    written = load_commits(df, TEST_CONFIG, connection=conn)
    assert written == len(df)

    rows = conn.execute("SELECT sha FROM commits").fetchall()
    assert len(rows) == len(df)


def test_load_is_idempotent_on_rerun(raw_commit_records):
    conn = sqlite3.connect(":memory:")
    df = transform_commits(raw_commit_records)

    load_commits(df, TEST_CONFIG, connection=conn)
    load_commits(df, TEST_CONFIG, connection=conn)  # re-run with identical data

    count = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
    assert count == len(df)


def test_load_upserts_changed_fields(raw_commit_records):
    conn = sqlite3.connect(":memory:")
    df = transform_commits(raw_commit_records)
    load_commits(df, TEST_CONFIG, connection=conn)

    df.loc[0, "message_subject"] = "Updated subject"
    load_commits(df, TEST_CONFIG, connection=conn)

    sha = df.iloc[0]["sha"]
    row = conn.execute(
        "SELECT message_subject FROM commits WHERE sha = ?", (sha,)
    ).fetchone()
    assert row[0] == "Updated subject"

    count = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
    assert count == len(df)


def test_load_empty_dataframe_is_noop(raw_commit_records):
    conn = sqlite3.connect(":memory:")
    df = transform_commits([])
    written = load_commits(df, TEST_CONFIG, connection=conn)
    assert written == 0


def test_load_creates_warehouse_file_on_disk(tmp_path, raw_commit_records):
    warehouse_path = tmp_path / "sub" / "warehouse.db"
    config = PipelineConfig(warehouse_path=str(warehouse_path), warehouse_table="commits")
    df = transform_commits(raw_commit_records)
    load_commits(df, config)

    assert warehouse_path.exists()
    conn = sqlite3.connect(warehouse_path)
    count = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
    assert count == len(df)
    conn.close()
