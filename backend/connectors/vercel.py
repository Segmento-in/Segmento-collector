import datetime
import json
import sqlite3
import os
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = os.getenv("DB_PATH", "/tmp/identity.db")
SOURCE = "vercel"
PROJECTS_SOURCE = "vercel_projects"
DEPLOYMENTS_SOURCE = "vercel_deployments"
DOMAINS_SOURCE = "vercel_domains"
API_BASE = "https://api.vercel.com"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[VERCEL] {message}", flush=True)


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
        if isinstance(value, (int, float)):
            return datetime.datetime.fromtimestamp(value / 1000, tz=datetime.UTC)
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


def save_config(uid: str, access_token: str):
    config = {"access_token": access_token.strip()}

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


def _get_headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
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
        raise Exception(f"Vercel API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


def _fetch_user(token: str) -> dict:
    return _request("GET", "/v2/user", token)


def _fetch_projects(token: str) -> list[dict]:
    results = []
    url = "/v9/projects?limit=100"
    while url:
        data = _request("GET", url, token)
        results.extend(data.get("projects", []))
        pagination = data.get("pagination") or {}
        next_cursor = pagination.get("next")
        if next_cursor:
            url = f"/v9/projects?limit=100&until={next_cursor}"
        else:
            url = None
    return results


def _fetch_deployments(token: str) -> list[dict]:
    results = []
    url = "/v6/deployments?limit=100"
    page = 0
    while url and page < 5:
        data = _request("GET", url, token)
        results.extend(data.get("deployments", []))
        pagination = data.get("pagination") or {}
        next_cursor = pagination.get("next")
        if next_cursor:
            url = f"/v6/deployments?limit=100&until={next_cursor}"
        else:
            url = None
        page += 1
    return results


def _fetch_domains(token: str) -> list[dict]:
    results = []
    url = "/v5/domains?limit=100"
    page = 0
    while url and page < 5:
        data = _request("GET", url, token)
        results.extend(data.get("domains", []))
        pagination = data.get("pagination") or {}
        next_cursor = pagination.get("next")
        if next_cursor:
            url = f"/v5/domains?limit=100&until={next_cursor}"
        else:
            url = None
        page += 1
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


def connect_vercel(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Vercel not configured for this user"}

    try:
        me = _fetch_user(cfg["access_token"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    user_obj = me.get("user") or {}
    username = user_obj.get("username") or user_obj.get("name") or "Vercel user"

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} username={username}")
    return {
        "status": "success",
        "access_token": _mask_token(cfg.get("access_token")),
        "username": username,
    }


def sync_vercel(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Vercel not configured"}

    token = cfg["access_token"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        projects = _fetch_projects(token)
        deployments = _fetch_deployments(token)
        domains = _fetch_domains(token)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    project_rows = []
    for project in projects:
        updated_at = _parse_dt(project.get("updatedAt"))
        if last_sync_at and updated_at and updated_at <= last_sync_at:
            continue
        project_rows.append(
            {
                "uid": uid,
                "source": PROJECTS_SOURCE,
                "project_id": project.get("id"),
                "name": project.get("name"),
                "framework": project.get("framework"),
                "node_version": project.get("nodeVersion"),
                "created_at": project.get("createdAt"),
                "updated_at": project.get("updatedAt"),
                "data_json": json.dumps(project, default=str),
                "raw_json": json.dumps(project, default=str),
                "fetched_at": fetched_at,
            }
        )

    deployment_rows = []
    for deployment in deployments:
        created_at = _parse_dt(deployment.get("createdAt"))
        if last_sync_at and created_at and created_at <= last_sync_at:
            continue
        deployment_rows.append(
            {
                "uid": uid,
                "source": DEPLOYMENTS_SOURCE,
                "deployment_id": deployment.get("uid"),
                "name": deployment.get("name"),
                "url": deployment.get("url"),
                "state": deployment.get("state"),
                "target": deployment.get("target"),
                "created_at": deployment.get("createdAt"),
                "ready_at": deployment.get("ready"),
                "data_json": json.dumps(deployment, default=str),
                "raw_json": json.dumps(deployment, default=str),
                "fetched_at": fetched_at,
            }
        )

    domain_rows = []
    for domain in domains:
        created_at = _parse_dt(domain.get("createdAt"))
        if last_sync_at and created_at and created_at <= last_sync_at:
            continue
        domain_rows.append(
            {
                "uid": uid,
                "source": DOMAINS_SOURCE,
                "domain_id": domain.get("id"),
                "name": domain.get("name"),
                "service_type": domain.get("serviceType"),
                "ns_verified_at": domain.get("nsVerifiedAt"),
                "txt_verified_at": domain.get("txtVerifiedAt"),
                "verified": bool(domain.get("verified")),
                "created_at": domain.get("createdAt"),
                "expires_at": domain.get("expiresAt"),
                "data_json": json.dumps(domain, default=str),
                "raw_json": json.dumps(domain, default=str),
                "fetched_at": fetched_at,
            }
        )

    total_rows_found += len(project_rows) + len(deployment_rows) + len(domain_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PROJECTS_SOURCE, project_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, DEPLOYMENTS_SOURCE, deployment_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, DOMAINS_SOURCE, domain_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "projects_found": len(project_rows),
        "deployments_found": len(deployment_rows),
        "domains_found": len(domain_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_vercel(uid: str) -> dict:
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
