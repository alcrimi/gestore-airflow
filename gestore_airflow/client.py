"""Client for interacting with the Airflow REST API on Google Cloud Composer.

Provides two main capabilities:
- Read DAG runs that are currently in *running* state and return their run_ids.
- Update the state of DAG runs identified by run_id.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Airflow DAG run states
STATE_RUNNING = "running"
STATE_SUCCESS = "success"
STATE_FAILED = "failed"
STATE_QUEUED = "queued"

VALID_STATES = {STATE_RUNNING, STATE_SUCCESS, STATE_FAILED, STATE_QUEUED}

# Maximum number of DAGs/runs to retrieve per page
_PAGE_LIMIT = 100


class AirflowAPIError(Exception):
    """Raised when the Airflow REST API returns an unexpected response."""


class AirflowManager:
    """Manages DAG runs on an Airflow instance via the REST API v1.

    Parameters
    ----------
    base_url:
        Base URL of the Airflow web server, e.g.
        ``"https://<airflow-host>"`` (without a trailing slash).
        For Google Cloud Composer, use the Composer environment web server URL.
    auth:
        Authentication object accepted by *requests*, e.g.
        ``requests.auth.HTTPBasicAuth("user", "password")`` or a
        ``google.auth.transport.requests.AuthorizedSession``-style callable.
        Pass ``None`` only when the Airflow instance does not require
        authentication (not recommended for production).
    timeout:
        Request timeout in seconds (default: 30).
    verify_ssl:
        Whether to verify TLS certificates (default: ``True``).
    """

    _API_PREFIX = "/api/v1"

    def __init__(
        self,
        base_url: str,
        auth: Any = None,
        timeout: int = 30,
        verify_ssl: bool = True,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._auth = auth
        self._timeout = timeout
        self._verify_ssl = verify_ssl

        self._session = self._build_session()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods={"GET", "PATCH"},
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        if self._auth is not None:
            session.auth = self._auth
        return session

    def _url(self, path: str) -> str:
        return f"{self._base_url}{self._API_PREFIX}{path}"

    def _get(self, path: str, params: dict | None = None) -> Any:
        url = self._url(path)
        logger.debug("GET %s params=%s", url, params)
        response = self._session.get(
            url,
            params=params,
            timeout=self._timeout,
            verify=self._verify_ssl,
        )
        if not response.ok:
            raise AirflowAPIError(
                f"GET {url} failed with status {response.status_code}: {response.text}"
            )
        return response.json()

    def _patch(self, path: str, json: dict) -> Any:
        url = self._url(path)
        logger.debug("PATCH %s body=%s", url, json)
        response = self._session.patch(
            url,
            json=json,
            timeout=self._timeout,
            verify=self._verify_ssl,
        )
        if not response.ok:
            raise AirflowAPIError(
                f"PATCH {url} failed with status {response.status_code}: {response.text}"
            )
        return response.json()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_dags(self) -> list[dict]:
        """Return a list of all DAG definitions available in Airflow.

        Returns
        -------
        list[dict]
            Each item is the raw DAG object returned by the Airflow API.
        """
        offset = 0
        dags: list[dict] = []
        while True:
            data = self._get("/dags", params={"limit": _PAGE_LIMIT, "offset": offset})
            batch = data.get("dags", [])
            dags.extend(batch)
            if len(dags) >= data.get("total_entries", len(dags)) or not batch:
                break
            offset += _PAGE_LIMIT
        logger.info("Fetched %d DAG(s)", len(dags))
        return dags

    def get_running_dag_runs(self) -> list[str]:
        """Return the run_ids of all DAG runs that are currently *running*.

        Iterates over all DAGs visible to the authenticated user and collects
        every DAG run whose ``state`` is ``"running"``.

        Returns
        -------
        list[str]
            A list of ``run_id`` strings for all currently running DAG runs.
        """
        running_run_ids: list[str] = []
        dags = self.get_dags()

        for dag in dags:
            dag_id = dag.get("dag_id")
            if not dag_id:
                continue
            offset = 0
            while True:
                data = self._get(
                    f"/dags/{dag_id}/dagRuns",
                    params={
                        "state": STATE_RUNNING,
                        "limit": _PAGE_LIMIT,
                        "offset": offset,
                    },
                )
                runs = data.get("dag_runs", [])
                for run in runs:
                    run_id = run.get("run_id")
                    if run_id:
                        running_run_ids.append(run_id)
                        logger.debug(
                            "Running DAG run found: dag_id=%s run_id=%s",
                            dag_id,
                            run_id,
                        )
                offset += len(runs)
                if offset >= data.get("total_entries", offset) or not runs:
                    break

        logger.info("Found %d running DAG run(s)", len(running_run_ids))
        return running_run_ids

    def update_dag_run_state(
        self, dag_id: str, run_id: str, state: str
    ) -> dict:
        """Update the state of a specific DAG run.

        Parameters
        ----------
        dag_id:
            The identifier of the DAG that owns the run.
        run_id:
            The ``run_id`` of the DAG run to update.
        state:
            The new state to set. Must be one of ``"success"``,
            ``"failed"``, ``"queued"``, or ``"running"``.

        Returns
        -------
        dict
            The updated DAG run object as returned by the Airflow API.

        Raises
        ------
        ValueError
            If *state* is not a recognised Airflow DAG run state.
        AirflowAPIError
            If the API call fails.
        """
        if state not in VALID_STATES:
            raise ValueError(
                f"Invalid state '{state}'. Must be one of: {sorted(VALID_STATES)}"
            )
        result = self._patch(
            f"/dags/{dag_id}/dagRuns/{run_id}",
            json={"state": state},
        )
        logger.info(
            "Updated DAG run state: dag_id=%s run_id=%s new_state=%s",
            dag_id,
            run_id,
            state,
        )
        return result

    def update_dag_runs_state(
        self, run_id_dag_id_pairs: list[tuple[str, str]], state: str
    ) -> list[dict]:
        """Update the state of multiple DAG runs at once.

        Parameters
        ----------
        run_id_dag_id_pairs:
            A list of ``(run_id, dag_id)`` tuples identifying each DAG run
            that should be updated.
        state:
            The new state to set for all listed runs. Must be one of
            ``"success"``, ``"failed"``, ``"queued"``, or ``"running"``.

        Returns
        -------
        list[dict]
            A list of updated DAG run objects returned by the Airflow API,
            one per successfully updated run.

        Raises
        ------
        ValueError
            If *state* is not a recognised Airflow DAG run state.
        """
        if state not in VALID_STATES:
            raise ValueError(
                f"Invalid state '{state}'. Must be one of: {sorted(VALID_STATES)}"
            )
        results: list[dict] = []
        for run_id, dag_id in run_id_dag_id_pairs:
            updated = self.update_dag_run_state(dag_id, run_id, state)
            results.append(updated)
        return results
