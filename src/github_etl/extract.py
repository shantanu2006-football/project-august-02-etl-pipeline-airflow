"""Extract raw commit activity from the public GitHub REST API.

Uses the unauthenticated (or token-authenticated, if ``GITHUB_TOKEN`` is set)
``GET /repos/{owner}/{repo}/commits`` endpoint, which is public for public
repositories and requires no API key. Pagination and transient-failure
retries are handled here so downstream stages only ever see a list of
already-fetched commit records.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from github_etl.config import DEFAULT_CONFIG, PipelineConfig

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class ExtractError(RuntimeError):
    """Raised when the GitHub API cannot be read after all retries are exhausted."""


def _build_headers(config: PipelineConfig) -> dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    if config.github_token:
        headers["Authorization"] = f"Bearer {config.github_token}"
    return headers


def _get_with_retries(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    config: PipelineConfig,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, config.max_retries + 1):
        try:
            response = session.get(
                url,
                params=params,
                headers=_build_headers(config),
                timeout=config.request_timeout_seconds,
            )
        except requests.RequestException as exc:  # network-level failure
            last_exc = exc
            logger.warning("Request error on attempt %d/%d: %s", attempt, config.max_retries, exc)
        else:
            if response.status_code == 200:
                return response
            if response.status_code not in _RETRYABLE_STATUS_CODES:
                raise ExtractError(
                    f"GitHub API returned non-retryable status {response.status_code}: "
                    f"{response.text[:300]}"
                )
            last_exc = ExtractError(
                f"GitHub API returned retryable status {response.status_code} on "
                f"attempt {attempt}/{config.max_retries}"
            )
            logger.warning(str(last_exc))

        if attempt < config.max_retries:
            time.sleep(config.retry_backoff_seconds * attempt)

    raise ExtractError(
        f"Failed to fetch {url} after {config.max_retries} attempts"
    ) from last_exc


def fetch_commits(config: PipelineConfig = DEFAULT_CONFIG) -> list[dict[str, Any]]:
    """Fetch commit records for the configured repository.

    Paginates through the commits endpoint up to ``config.max_pages`` pages
    of ``config.per_page`` commits each, so a single call can pull the full
    (bounded) recent history of a repository.

    Returns the raw JSON records as returned by GitHub, unmodified — cleaning
    and normalization happen in the transform stage.
    """
    records: list[dict[str, Any]] = []
    with requests.Session() as session:
        for page in range(1, config.max_pages + 1):
            params = {"per_page": config.per_page, "page": page}
            response = _get_with_retries(session, config.commits_endpoint, params, config)
            page_records = response.json()
            if not isinstance(page_records, list):
                raise ExtractError(
                    f"Unexpected response shape from GitHub API: {type(page_records).__name__}"
                )
            if not page_records:
                break
            records.extend(page_records)
            if len(page_records) < config.per_page:
                break

    logger.info("Fetched %d commit records for %s/%s", len(records), config.owner, config.repo)
    return records
