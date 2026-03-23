import datetime
import json
import sqlite3
import time
import base64

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "airflow"
DAGS_SOURCE = "airflow_dags"
DAG_RUNS_SOURCE = "airflow_dag_runs"
TASK_INSTANCES_SOURCE = "airflow_task_instances"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[AIRFLOW] {message}")


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * max(len(token) - 8, 4)}{token[-4:]}"


def _parse_dt(value):
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)
    except Exception:
        return None


def _iso_now():
    return datetime.datetime.now(datetime.UTC).isoformat()


def _get_config(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT config_json
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row or not row.get("config_json"):
        return None

    try:
        return json.loads(row["config_json"])
    except Exception:
        return None


def get_state(uid: str) -> dict:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row or not row.get("state_json"):
        return {"last_sync_at": None}

    try:
        return json.loads(row["state_json"])
    except Exception:
        return {"last_sync_at": None}


def save_state(uid: str, state: dict):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (uid, SOURCE, json.dumps(state), _iso_now()),
    )
    con.commit()
    con.close()


def _update_status(uid: str, status: str):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE connector_configs
        SET status=?
        WHERE uid=? AND connector=?
        """,
        (status, uid, SOURCE),
    )
    con.commit()
    con.close()


def _set_connection_enabled(uid: str, enabled: bool):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE google_connections
        SET enabled=?
        WHERE uid=? AND source=?
        """,
        (1 if enabled else 0, uid, SOURCE),
    )
    if cur.rowcount == 0:
        cur.execute(
            """
            INSERT INTO google_connections (uid, source, enabled)
            VALUES (?, ?, ?)
            """,
            (uid, SOURCE, 1 if enabled else 0),
        )
    con.commit()
    con.close()


def save_config(uid: str, base_url: str, username: str, password: str):
    config = {
        "base_url": base_url.strip().rstrip("/"),
        "username": username.strip(),
        "password": password.strip(),
    }

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, status, created_at)
        VALUES (?, ?, ?, 'pending', ?)
        """,
        (
            uid,
            SOURCE,
            encrypt_value(json.dumps(config)),
            _iso_now(),
        ),
    )
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}")


def _get_headers(username: str, password: str):
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, username: str, password: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_get_headers(username, password))

    for attempt in range(retries):
        response = requests.request(method, url, headers=headers, timeout=40, **kwargs)

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            try:
                wait_s = int(retry_after) if retry_after else 2 ** attempt
            except Exception:
                wait_s = 2 ** attempt
            if attempt == retries - 1:
                break
            _log(f"Rate limited on {url}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if response.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                break
            wait_s = min(2 ** attempt, 15)
            _log(f"Server error {response.status_code} on {url}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        break

    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Airflow API error {response.status_code}: {detail}")

    return response.json() if response.text else {}


def _fetch_dags(base_url: str, username: str, password: str):
    url = f"{base_url}/api/v1/dags"
    params = {"limit": 100}
    data = _request("GET", url, username, password, params=params)
    return data.get("dags", [])


def _fetch_dag_runs(base_url: str, username: str, password: str, dag_id: str, last_sync_at=None):
    url = f"{base_url}/api/v1/dags/{dag_id}/dagRuns"
    params = {"limit": 100}
    data = _request("GET", url, username, password, params=params)
    runs = data.get("dag_runs", [])
    
    if last_sync_at:
        filtered = []
        for run in runs:
            execution_date = _parse_dt(run.get("execution_date"))
            if execution_date and execution_date > last_sync_at:
                filtered.append(run)
        return filtered
    
    return runs


def _fetch_task_instances(base_url: str, username: str, password: str, dag_id: str, dag_run_id: str):
    url = f"{base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    data = _request("GET", url, username, password)
    return data.get("task_instances", [])


def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg:
        _log(f"No active destination configured. Skipping push for {label}")
        return 0
    if not rows:
        _log(f"No rows generated for {label}. Skipping push")
        return 0

    _log(
        f"Pushing {len(rows)} rows to destination "
        f"(route_source={route_source}, label={label}, dest_type={dest_cfg.get('type')})"
    )
    pushed = push_to_destination(dest_cfg, route_source, rows)
    _log(
        f"Destination push completed "
        f"(route_source={route_source}, label={label}, rows_pushed={pushed})"
    )
    return pushed


def connect_airflow(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Airflow not configured for this user"}

    try:
        dags = _fetch_dags(cfg["base_url"], cfg["username"], cfg["password"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid}")
    return {
        "status": "success",
        "dag_count": len(dags),
    }


def sync_airflow(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Airflow not configured"}

    base_url = cfg["base_url"]
    username = cfg["username"]
    password = cfg["password"]
    
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        dags = _fetch_dags(base_url, username, password)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process DAGs
    dag_rows = []
    for dag in dags:
        dag_rows.append({
            "uid": uid,
            "source": DAGS_SOURCE,
            "dag_id": dag.get("dag_id"),
            "is_paused": bool(dag.get("is_paused")),
            "is_active": bool(dag.get("is_active")),
            "schedule_interval": dag.get("schedule_interval"),
            "data_json": json.dumps(dag, default=str),
            "raw_json": json.dumps(dag, default=str),
            "fetched_at": fetched_at,
        })

    # Process DAG Runs and Task Instances
    dag_run_rows = []
    task_instance_rows = []
    
    for dag in dags[:10]:  # Limit to first 10 DAGs to avoid long sync
        dag_id = dag.get("dag_id")
        try:
            dag_runs = _fetch_dag_runs(base_url, username, password, dag_id, last_sync_at)
            
            for run in dag_runs:
                dag_run_rows.append({
                    "uid": uid,
                    "source": DAG_RUNS_SOURCE,
                    "dag_run_id": run.get("dag_run_id"),
                    "dag_id": dag_id,
                    "state": run.get("state"),
                    "execution_date": run.get("execution_date"),
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                    "data_json": json.dumps(run, default=str),
                    "raw_json": json.dumps(run, default=str),
                    "fetched_at": fetched_at,
                })
                
                # Fetch task instances for this run
                try:
                    tasks = _fetch_task_instances(base_url, username, password, dag_id, run.get("dag_run_id"))
                    for task in tasks:
                        task_instance_rows.append({
                            "uid": uid,
                            "source": TASK_INSTANCES_SOURCE,
                            "task_id": task.get("task_id"),
                            "dag_id": dag_id,
                            "dag_run_id": run.get("dag_run_id"),
                            "state": task.get("state"),
                            "start_date": task.get("start_date"),
                            "end_date": task.get("end_date"),
                            "duration": task.get("duration"),
                            "data_json": json.dumps(task, default=str),
                            "raw_json": json.dumps(task, default=str),
                            "fetched_at": fetched_at,
                        })
                except Exception as e:
                    _log(f"Failed to fetch tasks for run {run.get('dag_run_id')}: {e}")
                    
        except Exception as e:
            _log(f"Failed to fetch runs for DAG {dag_id}: {e}")
            continue

    total_rows_found += len(dag_rows) + len(dag_run_rows) + len(task_instance_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, DAGS_SOURCE, dag_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, DAG_RUNS_SOURCE, dag_run_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, TASK_INSTANCES_SOURCE, task_instance_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "dags_found": len(dag_rows),
        "dag_runs_found": len(dag_run_rows),
        "task_instances_found": len(task_instance_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_airflow(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}


def _get_active_destination(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()

    if not row:
        return None

    return {
        "type": row["dest_type"],
        "host": row["host"],
        "port": row["port"],
        "username": row["username"],
        "password": row["password"],
        "database_name": row["database_name"],
    }
