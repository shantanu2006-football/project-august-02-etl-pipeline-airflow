from __future__ import annotations

import pytest

from github_etl.transform import transform_commits
from github_etl.validate import DataQualityError, validate_commits


def test_validate_passes_on_clean_data(raw_commit_records):
    df = transform_commits(raw_commit_records)
    report = validate_commits(df)
    assert report.is_valid
    assert report.checks_failed == []
    assert report.row_count == len(df)


def test_validate_fails_on_empty_dataframe():
    df = transform_commits([])
    report = validate_commits(df)
    assert not report.is_valid
    assert any("empty" in msg for msg in report.checks_failed)


def test_validate_fails_on_null_in_required_column(raw_commit_records):
    df = transform_commits(raw_commit_records)
    df.loc[0, "author_name"] = None
    report = validate_commits(df)
    assert not report.is_valid
    assert any("author_name" in msg for msg in report.checks_failed)


def test_validate_fails_on_duplicate_sha(raw_commit_records):
    df = transform_commits(raw_commit_records)
    dup = df.iloc[[0]].copy()
    df_with_dupe = pd_concat_helper(df, dup)
    report = validate_commits(df_with_dupe)
    assert not report.is_valid
    assert any("duplicate" in msg for msg in report.checks_failed)


def test_validate_fails_on_missing_column(raw_commit_records):
    df = transform_commits(raw_commit_records).drop(columns=["message"])
    report = validate_commits(df)
    assert not report.is_valid
    assert any("missing required column" in msg for msg in report.checks_failed)


def test_raise_if_invalid_raises_data_quality_error(raw_commit_records):
    df = transform_commits(raw_commit_records)
    df.loc[0, "sha"] = None
    report = validate_commits(df)
    with pytest.raises(DataQualityError):
        report.raise_if_invalid()


def pd_concat_helper(*dfs):
    import pandas as pd

    return pd.concat(dfs, ignore_index=True)
