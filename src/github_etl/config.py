"""Central configuration for the pipeline, overridable via environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration for a single pipeline run.

    All values can be overridden via environment variables so the same code
    runs unchanged locally, in tests, in CI, and inside an Airflow task.
    """

    owner: str = os.environ.get("ETL_GITHUB_OWNER", "shantanu2006-football")
    repo: str = os.environ.get("ETL_GITHUB_REPO", "project-august-02-etl-pipeline-airflow")
    api_base_url: str = os.environ.get("ETL_GITHUB_API_BASE_URL", "https://api.github.com")
    per_page: int = int(os.environ.get("ETL_GITHUB_PER_PAGE", "100"))
    max_pages: int = int(os.environ.get("ETL_GITHUB_MAX_PAGES", "5"))
    request_timeout_seconds: float = float(os.environ.get("ETL_HTTP_TIMEOUT", "10"))
    max_retries: int = int(os.environ.get("ETL_HTTP_MAX_RETRIES", "3"))
    retry_backoff_seconds: float = float(os.environ.get("ETL_HTTP_RETRY_BACKOFF", "1.0"))
    warehouse_path: str = os.environ.get("ETL_WAREHOUSE_PATH", "warehouse/github_activity.db")
    warehouse_table: str = os.environ.get("ETL_WAREHOUSE_TABLE", "commits")
    github_token: str | None = os.environ.get("GITHUB_TOKEN") or None

    @property
    def commits_endpoint(self) -> str:
        return f"{self.api_base_url}/repos/{self.owner}/{self.repo}/commits"


DEFAULT_CONFIG = PipelineConfig()
