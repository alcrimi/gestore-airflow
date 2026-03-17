"""Unit tests for gestore_airflow.client.AirflowManager."""

import pytest
import requests_mock as rm

from gestore_airflow.client import (
    STATE_FAILED,
    STATE_QUEUED,
    STATE_RUNNING,
    STATE_SUCCESS,
    AirflowAPIError,
    AirflowManager,
)

BASE_URL = "https://airflow.example.com"
API = f"{BASE_URL}/api/v1"


@pytest.fixture()
def manager():
    """Return a manager pointed at a fake host."""
    return AirflowManager(base_url=BASE_URL)


# ---------------------------------------------------------------------------
# Helper to build DAG / DAG-run payloads
# ---------------------------------------------------------------------------

def _dags_response(*dag_ids: str) -> dict:
    return {
        "dags": [{"dag_id": d} for d in dag_ids],
        "total_entries": len(dag_ids),
    }


def _dag_runs_response(*run_ids: str, state: str = STATE_RUNNING) -> dict:
    return {
        "dag_runs": [{"run_id": r, "state": state} for r in run_ids],
        "total_entries": len(run_ids),
    }


# ---------------------------------------------------------------------------
# get_dags
# ---------------------------------------------------------------------------

class TestGetDags:
    def test_returns_all_dags_single_page(self, manager):
        with rm.Mocker() as m:
            m.get(f"{API}/dags", json=_dags_response("dag_a", "dag_b"))
            result = manager.get_dags()
        assert [d["dag_id"] for d in result] == ["dag_a", "dag_b"]

    def test_paginates_when_total_exceeds_limit(self, manager):
        """Verify that the client fetches additional pages when needed."""
        first_page = {
            "dags": [{"dag_id": f"dag_{i}"} for i in range(100)],
            "total_entries": 110,
        }
        second_page = {
            "dags": [{"dag_id": f"dag_{i}"} for i in range(100, 110)],
            "total_entries": 110,
        }
        with rm.Mocker() as m:
            m.get(f"{API}/dags", [
                {"json": first_page, "status_code": 200},
                {"json": second_page, "status_code": 200},
            ])
            result = manager.get_dags()
        assert len(result) == 110

    def test_raises_on_api_error(self, manager):
        with rm.Mocker() as m:
            m.get(f"{API}/dags", status_code=500, text="Internal Server Error")
            with pytest.raises(AirflowAPIError):
                manager.get_dags()


# ---------------------------------------------------------------------------
# get_running_dag_runs
# ---------------------------------------------------------------------------

class TestGetRunningDagRuns:
    def test_returns_run_ids_of_running_runs(self, manager):
        with rm.Mocker() as m:
            m.get(f"{API}/dags", json=_dags_response("dag_a", "dag_b"))
            m.get(
                f"{API}/dags/dag_a/dagRuns",
                json=_dag_runs_response("run_a_1", "run_a_2"),
            )
            m.get(
                f"{API}/dags/dag_b/dagRuns",
                json=_dag_runs_response("run_b_1"),
            )
            result = manager.get_running_dag_runs()
        assert set(result) == {"run_a_1", "run_a_2", "run_b_1"}

    def test_returns_empty_list_when_no_running_runs(self, manager):
        with rm.Mocker() as m:
            m.get(f"{API}/dags", json=_dags_response("dag_a"))
            m.get(
                f"{API}/dags/dag_a/dagRuns",
                json={"dag_runs": [], "total_entries": 0},
            )
            result = manager.get_running_dag_runs()
        assert result == []

    def test_returns_empty_list_when_no_dags(self, manager):
        with rm.Mocker() as m:
            m.get(f"{API}/dags", json={"dags": [], "total_entries": 0})
            result = manager.get_running_dag_runs()
        assert result == []

    def test_skips_runs_without_run_id(self, manager):
        with rm.Mocker() as m:
            m.get(f"{API}/dags", json=_dags_response("dag_a"))
            m.get(
                f"{API}/dags/dag_a/dagRuns",
                json={"dag_runs": [{"state": STATE_RUNNING}], "total_entries": 1},
            )
            result = manager.get_running_dag_runs()
        assert result == []

    def test_raises_on_dag_runs_api_error(self, manager):
        with rm.Mocker() as m:
            m.get(f"{API}/dags", json=_dags_response("dag_a"))
            m.get(f"{API}/dags/dag_a/dagRuns", status_code=403, text="Forbidden")
            with pytest.raises(AirflowAPIError):
                manager.get_running_dag_runs()


# ---------------------------------------------------------------------------
# update_dag_run_state
# ---------------------------------------------------------------------------

class TestUpdateDagRunState:
    @pytest.mark.parametrize("new_state", [
        STATE_SUCCESS, STATE_FAILED, STATE_QUEUED, STATE_RUNNING
    ])
    def test_updates_state_successfully(self, manager, new_state):
        dag_id = "dag_a"
        run_id = "run_a_1"
        expected_response = {"run_id": run_id, "state": new_state}
        with rm.Mocker() as m:
            m.patch(
                f"{API}/dags/{dag_id}/dagRuns/{run_id}",
                json=expected_response,
            )
            result = manager.update_dag_run_state(dag_id, run_id, new_state)
        assert result == expected_response

    def test_raises_on_invalid_state(self, manager):
        with pytest.raises(ValueError, match="Invalid state"):
            manager.update_dag_run_state("dag_a", "run_a_1", "invalid_state")

    def test_raises_on_api_error(self, manager):
        with rm.Mocker() as m:
            m.patch(
                f"{API}/dags/dag_a/dagRuns/run_a_1",
                status_code=404,
                text="Not Found",
            )
            with pytest.raises(AirflowAPIError):
                manager.update_dag_run_state("dag_a", "run_a_1", STATE_SUCCESS)


# ---------------------------------------------------------------------------
# update_dag_runs_state (bulk)
# ---------------------------------------------------------------------------

class TestUpdateDagRunsState:
    def test_updates_multiple_runs(self, manager):
        pairs = [("run_a_1", "dag_a"), ("run_b_1", "dag_b")]
        with rm.Mocker() as m:
            m.patch(
                f"{API}/dags/dag_a/dagRuns/run_a_1",
                json={"run_id": "run_a_1", "state": STATE_SUCCESS},
            )
            m.patch(
                f"{API}/dags/dag_b/dagRuns/run_b_1",
                json={"run_id": "run_b_1", "state": STATE_SUCCESS},
            )
            results = manager.update_dag_runs_state(pairs, STATE_SUCCESS)
        assert len(results) == 2
        assert all(r["state"] == STATE_SUCCESS for r in results)

    def test_returns_empty_list_for_empty_input(self, manager):
        results = manager.update_dag_runs_state([], STATE_SUCCESS)
        assert results == []

    def test_raises_on_invalid_state(self, manager):
        with pytest.raises(ValueError, match="Invalid state"):
            manager.update_dag_runs_state([("run_a_1", "dag_a")], "bad_state")

    def test_raises_on_api_error_for_any_run(self, manager):
        pairs = [("run_a_1", "dag_a"), ("run_b_1", "dag_b")]
        with rm.Mocker() as m:
            m.patch(
                f"{API}/dags/dag_a/dagRuns/run_a_1",
                json={"run_id": "run_a_1", "state": STATE_FAILED},
            )
            m.patch(
                f"{API}/dags/dag_b/dagRuns/run_b_1",
                status_code=500,
                text="Server Error",
            )
            with pytest.raises(AirflowAPIError):
                manager.update_dag_runs_state(pairs, STATE_FAILED)
