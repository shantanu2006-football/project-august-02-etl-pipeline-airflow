from __future__ import annotations

import pytest
import responses

from github_etl.config import PipelineConfig
from github_etl.extract import ExtractError, fetch_commits

TEST_CONFIG = PipelineConfig(
    owner="example",
    repo="repo",
    per_page=2,
    max_pages=3,
    max_retries=2,
    retry_backoff_seconds=0,
)


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
def test_fetch_commits_single_page():
    responses.add(
        responses.GET,
        TEST_CONFIG.commits_endpoint,
        json=[_commit("a"), _commit("b")],
        status=200,
    )
    records = fetch_commits(TEST_CONFIG)
    # A short first page (< per_page requested... here equal, so a second empty page is fetched)
    assert len(records) >= 2


@responses.activate
def test_fetch_commits_paginates_until_short_page():
    responses.add(responses.GET, TEST_CONFIG.commits_endpoint, json=[_commit("a"), _commit("b")], status=200)
    responses.add(responses.GET, TEST_CONFIG.commits_endpoint, json=[_commit("c")], status=200)
    records = fetch_commits(TEST_CONFIG)
    assert [r["sha"] for r in records] == ["a", "b", "c"]


@responses.activate
def test_fetch_commits_stops_at_empty_page():
    responses.add(responses.GET, TEST_CONFIG.commits_endpoint, json=[], status=200)
    records = fetch_commits(TEST_CONFIG)
    assert records == []


@responses.activate
def test_fetch_commits_retries_on_retryable_status_then_succeeds():
    responses.add(responses.GET, TEST_CONFIG.commits_endpoint, status=503)
    responses.add(responses.GET, TEST_CONFIG.commits_endpoint, json=[_commit("a")], status=200)
    records = fetch_commits(TEST_CONFIG)
    assert [r["sha"] for r in records] == ["a"]


@responses.activate
def test_fetch_commits_raises_on_non_retryable_status():
    responses.add(responses.GET, TEST_CONFIG.commits_endpoint, status=404)
    with pytest.raises(ExtractError):
        fetch_commits(TEST_CONFIG)


@responses.activate
def test_fetch_commits_raises_after_exhausting_retries():
    responses.add(responses.GET, TEST_CONFIG.commits_endpoint, status=500)
    responses.add(responses.GET, TEST_CONFIG.commits_endpoint, status=500)
    with pytest.raises(ExtractError):
        fetch_commits(TEST_CONFIG)


@responses.activate
def test_fetch_commits_respects_max_pages():
    config = PipelineConfig(
        owner="example", repo="repo", per_page=1, max_pages=2, max_retries=1, retry_backoff_seconds=0
    )
    responses.add(responses.GET, config.commits_endpoint, json=[_commit("a")], status=200)
    responses.add(responses.GET, config.commits_endpoint, json=[_commit("b")], status=200)
    records = fetch_commits(config)
    assert len(records) == 2
    assert len(responses.calls) == 2
