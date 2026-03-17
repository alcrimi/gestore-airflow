# gestore-airflow

Gestione dei DAG su Google Cloud Composer (Airflow) tramite le API REST.

## Funzionalità

- **`get_running_dag_runs()`** – Legge quali DAG run sono in stato `running` e restituisce la lista dei loro `run_id`.
- **`update_dag_run_state(dag_id, run_id, state)`** – Aggiorna lo stato di un singolo DAG run.
- **`update_dag_runs_state(run_id_dag_id_pairs, state)`** – Aggiorna lo stato di più DAG run in una sola chiamata.

## Installazione

```bash
pip install -r requirements.txt
```

## Utilizzo

```python
from requests.auth import HTTPBasicAuth
from gestore_airflow import AirflowManager

# Crea il client puntando all'URL del web server Airflow
manager = AirflowManager(
    base_url="https://<airflow-webserver-url>",
    auth=HTTPBasicAuth("username", "password"),
)

# 1. Ottieni la lista dei run_id attualmente in esecuzione
running_ids = manager.get_running_dag_runs()
print("DAG run in esecuzione:", running_ids)

# 2. Aggiorna lo stato di un singolo DAG run
manager.update_dag_run_state(dag_id="my_dag", run_id=running_ids[0], state="failed")

# 3. Aggiorna lo stato di più DAG run contemporaneamente
#    La lista contiene coppie (run_id, dag_id)
pairs = [(run_id, "my_dag") for run_id in running_ids]
manager.update_dag_runs_state(pairs, state="failed")
```

### Autenticazione con Google Cloud Composer

Per Composer puoi usare `google-auth` per ottenere un token OAuth2:

```python
import google.auth
import google.auth.transport.requests
from gestore_airflow import AirflowManager

credentials, _ = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
auth_req = google.auth.transport.requests.Request()
credentials.refresh(auth_req)

manager = AirflowManager(
    base_url="https://<composer-webserver-url>",
    auth=lambda r: r.headers.update(
        {"Authorization": f"Bearer {credentials.token}"}
    ),
)
```

## Test

```bash
pip install -r requirements-dev.txt
pytest tests/
```

## Stati validi

| Stato      | Descrizione                        |
|------------|------------------------------------|
| `running`  | Il DAG run è in esecuzione         |
| `success`  | Il DAG run è completato con successo |
| `failed`   | Il DAG run è fallito               |
| `queued`   | Il DAG run è in coda               |