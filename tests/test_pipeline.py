from __future__ import annotations

import sqlite3

import responses

from github_etl.config import PipelineConfig
from github_etl.pipeline import run_pipeline


def _commit(sha: str) -> dict:
    return {
        "sha": sha,
        "html_url": f"https://github.com/example/repo/commit/{sha}",
        "commit": {
            "author": {"name": "Dev", "email": "dev@example.com", "date": "2026-01-01T00:00:00Z"},
            "message": "msg",
        },
        "author": {"login": "dev"},
    }


@responses.activate
def test_run_pipeline_end_to_end(tmp_path):
    config = PipelineConfig(
        owner="example",
        repo="repo",
        per_page=10,
        max_pages=1,
        max_retries=1,
        retry_backoff_seconds=0,
        warehouse_path=str(tmp_path / "warehouse.db"),
        warehouse_table="commits",
    )
    responses.add(
        responses.GET,
        config.commits_endpoint,
        json=[_commit("x1"), _commit("x2")],
        status=200,
    )

    result = run_pipeline(config)

    assert result.extracted_count == 2
    assert result.transformed_count == 2
    assert result.loaded_count == 2
    assert result.validation_passed is True

    conn = sqlite3.connect(config.warehouse_path)
    count = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
    assert count == 2
    conn.close()


@responses.activate
def test_run_pipeline_is_idempotent(tmp_path):
    config = PipelineConfig(
        owner="example",
        repo="repo",
        per_page=10,
        max_pages=1,
        max_retries=1,
        retry_backoff_seconds=0,
        warehouse_path=str(tmp_path / "warehouse.db"),
        warehouse_table="commits",
    )
    responses.add(responses.GET, config.commits_endpoint, json=[_commit("x1")], status=200)
    responses.add(responses.GET, config.commits_endpoint, json=[_commit("x1")], status=200)

    run_pipeline(config)
    run_pipeline(config)

    conn = sqlite3.connect(config.warehouse_path)
    count = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
    conn.close()
    assert count == 1


@responses.activate
def test_run_pipeline_skips_load_when_no_commits(tmp_path):
    config = PipelineConfig(
        owner="example",
        repo="repo",
        per_page=10,
        max_pages=1,
        max_retries=1,
        retry_backoff_seconds=0,
        warehouse_path=str(tmp_path / "warehouse.db"),
        warehouse_table="commits",
    )
    responses.add(responses.GET, config.commits_endpoint, json=[], status=200)

    result = run_pipeline(config)

    assert result.extracted_count == 0
    assert result.loaded_count == 0
    assert not (tmp_path / "warehouse.db").exists()
