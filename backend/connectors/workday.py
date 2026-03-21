import base64
import datetime
import json
import sqlite3

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = "identity.db"
SOURCE = "workday"
WORKERS_SOURCE = "workday_workers"
ORGANIZATIONS_SOURCE = "workday_organizations"
JOBS_SOURCE = "workday_jobs"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _iso_now():
    return datetime.datetime.now(datetime.UTC).isoformat()


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


def _normalize_base_url(base_url: str) -> str:
    url = (base_url or "").strip().rstrip("/")
    if not url:
        return ""
    if "/ccx/api/v1" not in url:
        url = f"{url}/ccx/api/v1"
    return url


def _mask_secret(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:2]}{'*' * max(len(value) - 4, 4)}{value[-2:]}"


def _get_config(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector=? LIMIT 1",
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
        "SELECT state_json FROM connector_state WHERE uid=? AND source=? LIMIT 1",
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
        "UPDATE connector_configs SET status=? WHERE uid=? AND connector=?",
        (status, uid, SOURCE),
    )
    con.commit()
    con.close()


def _set_connection_enabled(uid: str, enabled: bool):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "UPDATE google_connections SET enabled=? WHERE uid=? AND source=?",
        (1 if enabled else 0, uid, SOURCE),
    )
    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO google_connections (uid, source, enabled) VALUES (?, ?, ?)",
            (uid, SOURCE, 1 if enabled else 0),
        )
    con.commit()
    con.close()


def save_config(uid: str, payload: dict):
    config = {
        "username": (payload.get("username") or "").strip(),
        "password": (payload.get("password") or "").strip(),
        "base_url": _normalize_base_url(payload.get("base_url") or ""),
    }
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, status, created_at)
        VALUES (?, ?, ?, 'pending', ?)
        """,
        (uid, SOURCE, encrypt_value(json.dumps(config)), _iso_now()),
    )
    con.commit()
    con.close()


def _request(cfg: dict, path: str, params=None) -> dict:
    basic = base64.b64encode(f"{cfg['username']}:{cfg['password']}".encode("utf-8")).decode("utf-8")
    response = requests.get(
        f"{cfg['base_url']}/{path.lstrip('/')}",
        headers={"Authorization": f"Basic {basic}", "Accept": "application/json"},
        params=params,
        timeout=10,
    )
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Workday API error {response.status_code}: {detail}")
    return response.json() if response.text else {}


def _extract_rows(payload: dict) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "workers", "organizations", "jobs"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return [payload]
    return []


def _filter_incremental(rows: list[dict], last_sync_at):
    if not last_sync_at:
        return rows
    filtered = []
    for row in rows:
        updated_at = _parse_dt(row.get("updatedDate") or row.get("lastUpdated") or row.get("updated_at"))
        if not updated_at or updated_at > last_sync_at:
            filtered.append(row)
    return filtered


def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg or not rows:
        return 0
    return push_to_destination(dest_cfg, route_source, rows)


def connect_workday(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Workday not configured"}
    try:
        workers = _extract_rows(_request(cfg, "workers", params={"limit": 1}))
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}
    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    return {
        "status": "success",
        "username": cfg.get("username"),
        "password": _mask_secret(cfg.get("password")),
        "base_url": cfg.get("base_url"),
        "workers_visible": len(workers),
    }


def sync_workday(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Workday not configured"}
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None
    try:
        workers = _extract_rows(_request(cfg, "workers", params={"limit": 100}))
        organizations = _extract_rows(_request(cfg, "organizations", params={"limit": 100}))
        jobs = _extract_rows(_request(cfg, "jobs", params={"limit": 100}))
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    workers = _filter_incremental(workers, last_sync_at)
    organizations = _filter_incremental(organizations, last_sync_at)
    jobs = _filter_incremental(jobs, last_sync_at)

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    worker_rows = [{
        "uid": uid,
        "source": WORKERS_SOURCE,
        "worker_id": item.get("id") or item.get("workerId") or item.get("descriptor"),
        "employee_id": item.get("employeeId"),
        "name": item.get("descriptor") or item.get("name"),
        "organization_id": item.get("organizationId"),
        "job_id": item.get("jobId"),
        "updated_at": item.get("updatedDate") or item.get("lastUpdated"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in workers]

    organization_rows = [{
        "uid": uid,
        "source": ORGANIZATIONS_SOURCE,
        "organization_id": item.get("id") or item.get("organizationId") or item.get("descriptor"),
        "name": item.get("descriptor") or item.get("name"),
        "type": item.get("type"),
        "manager_id": item.get("managerId"),
        "updated_at": item.get("updatedDate") or item.get("lastUpdated"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in organizations]

    job_rows = [{
        "uid": uid,
        "source": JOBS_SOURCE,
        "job_id": item.get("id") or item.get("jobId") or item.get("descriptor"),
        "title": item.get("descriptor") or item.get("title"),
        "job_profile_id": item.get("jobProfileId"),
        "worker_id": item.get("workerId"),
        "organization_id": item.get("organizationId"),
        "updated_at": item.get("updatedDate") or item.get("lastUpdated"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in jobs]

    rows_found = len(worker_rows) + len(organization_rows) + len(job_rows)
    rows_pushed = 0
    rows_pushed += _push_rows(dest_cfg, SOURCE, WORKERS_SOURCE, worker_rows)
    rows_pushed += _push_rows(dest_cfg, SOURCE, ORGANIZATIONS_SOURCE, organization_rows)
    rows_pushed += _push_rows(dest_cfg, SOURCE, JOBS_SOURCE, job_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "workers_found": len(worker_rows),
        "organizations_found": len(organization_rows),
        "jobs_found": len(job_rows),
        "rows_found": rows_found,
        "rows_pushed": rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_workday(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
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
