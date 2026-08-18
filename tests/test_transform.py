from __future__ import annotations

import pandas as pd

from github_etl.transform import OUTPUT_COLUMNS, transform_commits


def test_transform_produces_expected_columns(raw_commit_records):
    df = transform_commits(raw_commit_records)
    assert list(df.columns) == OUTPUT_COLUMNS


def test_transform_dedupes_by_sha(raw_commit_records):
    # Fixture contains 4 records but one sha is repeated.
    df = transform_commits(raw_commit_records)
    assert len(df) == 3
    assert df["sha"].is_unique


def test_transform_flags_merge_commits(raw_commit_records):
    df = transform_commits(raw_commit_records)
    merge_row = df[df["sha"] == "bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222"].iloc[0]
    assert bool(merge_row["is_merge_commit"]) is True

    non_merge_row = df[df["sha"] == "cccc3333cccc3333cccc3333cccc3333cccc3333"].iloc[0]
    assert bool(non_merge_row["is_merge_commit"]) is False


def test_transform_handles_missing_github_account(raw_commit_records):
    df = transform_commits(raw_commit_records)
    merge_row = df[df["sha"] == "bbbb2222bbbb2222bbbb2222bbbb2222bbbb2222"].iloc[0]
    assert pd.isna(merge_row["author_login"])
    # But the git-level author info still comes through.
    assert merge_row["author_name"] == "John Smith"


def test_transform_parses_committed_at_as_utc_timestamp(raw_commit_records):
    df = transform_commits(raw_commit_records)
    assert isinstance(df["committed_at"].dtype, pd.DatetimeTZDtype)
    assert str(df["committed_at"].dt.tz) == "UTC"


def test_transform_sorts_newest_first(raw_commit_records):
    df = transform_commits(raw_commit_records)
    dates = df["committed_at"].tolist()
    assert dates == sorted(dates, reverse=True)


def test_transform_drops_records_missing_sha():
    records = [
        {"sha": None, "commit": {"author": {"name": "x", "email": "x@example.com", "date": "2026-01-01T00:00:00Z"}, "message": "no sha"}},
        {"sha": "deadbeef", "commit": {"author": {"name": "x", "email": "x@example.com", "date": "2026-01-01T00:00:00Z"}, "message": "ok"}},
    ]
    df = transform_commits(records)
    assert len(df) == 1
    assert df.iloc[0]["sha"] == "deadbeef"


def test_transform_empty_input_returns_empty_dataframe_with_schema():
    df = transform_commits([])
    assert df.empty
    assert list(df.columns) == OUTPUT_COLUMNS
