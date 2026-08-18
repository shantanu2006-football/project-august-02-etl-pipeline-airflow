from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

# Airflow reads AIRFLOW_HOME at import time, so this must happen before any
# `airflow` import occurs anywhere in the test session (including inside the
# DAG file under test) — set it here since conftest.py always loads first.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow_home_"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(Path(__file__).parent.parent / "dags"))

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def raw_commit_records() -> list[dict]:
    with open(FIXTURES_DIR / "sample_commits.json") as f:
        return json.load(f)
