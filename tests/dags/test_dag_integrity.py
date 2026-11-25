"""DAG integrity tests. Run inside the Astro project with:  astro dev pytest"""

import os

import pytest
from airflow.models import DagBag

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "dags")


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder=DAGS_DIR)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_initial_load_dag_shape(dagbag):
    dag = dagbag.dags["ecommerce_initial_load"]
    expected = [
        "check_raw_files", "create_dwh_tables", "extract_to_bronze", "validate_bronze",
        "transform_to_silver", "truncate_dwh", "build_and_load_gold", "create_views",
        "quality_checks",
    ]
    assert set(dag.task_ids) == set(expected)
    for upstream, downstream in zip(expected, expected[1:], strict=False):
        assert downstream in dag.task_dict[upstream].downstream_task_ids


def test_every_task_has_retries_or_is_a_gate(dagbag):
    gates = {"check_raw_files", "validate_bronze", "quality_checks"}
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            if task.task_id not in gates:
                assert task.retries >= 1, f"{dag.dag_id}.{task.task_id} has no retries"
