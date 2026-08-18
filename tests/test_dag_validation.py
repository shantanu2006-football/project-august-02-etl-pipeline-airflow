"""DAG-level tests using Airflow's own DagBag loader.

These catch the class of bugs unit tests on the ``github_etl`` package
cannot: import errors, cycles, missing task dependencies, and DAG-level
misconfiguration (schedule, retries, etc.) in the actual file Airflow will
parse in production.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle

DAGS_FOLDER = Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_dagbag_has_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dag_is_loaded(dagbag):
    assert "github_commits_etl" in dagbag.dags


def test_dag_has_expected_tasks_in_order(dagbag):
    dag = dagbag.dags["github_commits_etl"]
    task_ids = {t.task_id for t in dag.tasks}
    assert task_ids == {"extract", "transform", "validate", "load"}


def test_dag_task_dependency_chain(dagbag):
    dag = dagbag.dags["github_commits_etl"]
    extract = dag.get_task("extract")
    transform = dag.get_task("transform")
    validate = dag.get_task("validate")
    load = dag.get_task("load")

    assert transform.upstream_task_ids == {"extract"}
    assert validate.upstream_task_ids == {"transform"}
    assert load.upstream_task_ids == {"validate"}


def test_dag_has_no_cycles(dagbag):
    dag = dagbag.dags["github_commits_etl"]
    # check_cycle() raises AirflowDagCycleException if a cycle exists.
    check_cycle(dag)


def test_dag_has_retries_configured(dagbag):
    dag = dagbag.dags["github_commits_etl"]
    for task in dag.tasks:
        assert task.retries >= 1


def test_dag_has_no_catchup(dagbag):
    dag = dagbag.dags["github_commits_etl"]
    assert dag.catchup is False


def test_dag_id_matches_filename_convention(dagbag):
    dag = dagbag.dags["github_commits_etl"]
    assert Path(dag.fileloc).name == "github_commits_etl_dag.py"
