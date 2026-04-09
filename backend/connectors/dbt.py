import datetime
import json
import sqlite3
import os
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = os.getenv("DB_PATH", "identity.db")
SOURCE = "dbt"
JOBS_SOURCE = "dbt_jobs"
RUNS_SOURCE = "dbt_runs"
MODELS_SOURCE = "dbt_models"
API_BASE = "https://cloud.getdbt.com/api/v2"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[DBT] {message}", flush=True)


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
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def _iso_now():
    return datetime.datetime.utcnow().isoformat()


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


def save_config(uid: str, api_token: str, account_id: str):
    config = {
        "api_token": api_token.strip(),
        "account_id": account_id.strip(),
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


def _get_headers(token: str) -> dict:
    return {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, token: str, retries: int = 4, **kwargs):
    url = f"{API_BASE}{path}"
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_get_headers(token))

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
            _log(f"Rate limited on {path}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if response.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                break
            wait_s = min(2 ** attempt, 15)
            _log(f"Server error {response.status_code} on {path}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        break

    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"dbt Cloud API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


def _fetch_paginated(token: str, path: str) -> list[dict]:
    results = []
    offset = 0
    limit = 100

    while True:
        data = _request("GET", path, token, params={"offset": offset, "limit": limit})
        batch = data.get("data") or []
        results.extend(batch)

        total_count = (data.get("extra") or {}).get("pagination", {}).get("total_count", 0)
        offset += len(batch)

        if not batch or offset >= total_count:
            break

        time.sleep(0.1)

    return results


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


def connect_dbt(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "dbt not configured for this user"}

    try:
        data = _request("GET", f"/accounts/{cfg['account_id']}/", cfg["api_token"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    account_data = data.get("data") or {}
    account_name = account_data.get("name") or "dbt Cloud account"

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} account={account_name}")
    return {
        "status": "success",
        "api_token": _mask_token(cfg.get("api_token")),
        "account_id": cfg.get("account_id"),
        "account_name": account_name,
    }


def sync_dbt(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "dbt not configured"}

    token = cfg["api_token"]
    account_id = cfg["account_id"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        raw_jobs = _fetch_paginated(token, f"/accounts/{account_id}/jobs/")
        raw_runs = _fetch_paginated(token, f"/accounts/{account_id}/runs/")
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    if last_sync_at:
        raw_runs = [
            r for r in raw_runs
            if (_parse_dt(r.get("created_at")) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)) > last_sync_at
        ]

    job_rows = []
    for job in raw_jobs:
        job_rows.append({
            "uid": uid,
            "source": JOBS_SOURCE,
            "job_id": job.get("id"),
            "account_id": job.get("account_id"),
            "project_id": job.get("project_id"),
            "environment_id": job.get("environment_id"),
            "name": job.get("name"),
            "description": job.get("description"),
            "execute_steps": json.dumps(job.get("execute_steps") or [], default=str),
            "state": job.get("state"),
            "dbt_version": job.get("dbt_version"),
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at"),
            "data_json": json.dumps(job, default=str),
            "raw_json": json.dumps(job, default=str),
            "fetched_at": fetched_at,
        })

    run_rows = []
    model_rows = []
    for run in raw_runs:
        run_id = run.get("id")
        run_rows.append({
            "uid": uid,
            "source": RUNS_SOURCE,
            "run_id": run_id,
            "job_id": run.get("job_id"),
            "account_id": run.get("account_id"),
            "project_id": run.get("project_id"),
            "environment_id": run.get("environment_id"),
            "status": run.get("status"),
            "status_message": run.get("status_message"),
            "dbt_version": run.get("dbt_version"),
            "started_at": run.get("started_at"),
            "finished_at": run.get("finished_at"),
            "created_at": run.get("created_at"),
            "duration": run.get("duration"),
            "queued_duration": run.get("queued_duration"),
            "run_duration": run.get("run_duration"),
            "data_json": json.dumps(run, default=str),
            "raw_json": json.dumps(run, default=str),
            "fetched_at": fetched_at,
        })

        # Fetch models for each run
        if run_id:
            try:
                models_data = _request(
                    "GET",
                    f"/accounts/{account_id}/runs/{run_id}/artifacts/manifest.json",
                    token,
                )
                nodes = (models_data.get("nodes") or {})
                for node_key, node in nodes.items():
                    if node.get("resource_type") == "model":
                        model_rows.append({
                            "uid": uid,
                            "source": MODELS_SOURCE,
                            "run_id": run_id,
                            "job_id": run.get("job_id"),
                            "unique_id": node.get("unique_id"),
                            "name": node.get("name"),
                            "schema": node.get("schema"),
                            "database": node.get("database"),
                            "package_name": node.get("package_name"),
                            "path": node.get("path"),
                            "materialized": (node.get("config") or {}).get("materialized"),
                            "description": node.get("description"),
                            "data_json": json.dumps(node, default=str),
                            "raw_json": json.dumps(node, default=str),
                            "fetched_at": fetched_at,
                        })
            except Exception as exc:
                _log(f"Could not fetch models for run {run_id}: {exc}")

    total_rows_found = len(job_rows) + len(run_rows) + len(model_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, JOBS_SOURCE, job_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, RUNS_SOURCE, run_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, MODELS_SOURCE, model_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "jobs_found": len(job_rows),
        "runs_found": len(run_rows),
        "models_found": len(model_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_dbt(uid: str) -> dict:
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

