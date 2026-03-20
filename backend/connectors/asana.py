import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "asana"
TASKS_SOURCE = "asana_tasks"
PROJECTS_SOURCE = "asana_projects"
API_BASE = "https://app.asana.com/api/1.0"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[ASANA] {message}", flush=True)


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
        "Accept": "application/json",
    }


def _request(method: str, url: str, token: str, retries: int = 4, **kwargs):
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
        raise Exception(f"Asana API error {response.status_code}: {detail}")

    return response.json() if response.text else {}


def _fetch_workspaces(token: str) -> list[dict]:
    """Fetch user's workspaces"""
    url = f"{API_BASE}/workspaces"
    data = _request("GET", url, token)
    return data.get("data", [])


def _fetch_projects(token: str, workspace_gid: str) -> list[dict]:
    """Fetch all projects in workspace"""
    url = f"{API_BASE}/projects"
    params = {"workspace": workspace_gid, "limit": 100}
    
    all_projects = []
    while True:
        data = _request("GET", url, token, params=params)
        projects = data.get("data", [])
        all_projects.extend(projects)
        
        next_page = data.get("next_page")
        if not next_page or not next_page.get("uri"):
            break
        url = f"https://app.asana.com{next_page['uri']}"
        params = {}
    
    return all_projects


def _fetch_tasks(token: str, workspace_gid: str, last_sync_at: datetime.datetime | None = None) -> list[dict]:
    """Fetch tasks from workspace"""
    url = f"{API_BASE}/tasks"
    params = {"workspace": workspace_gid, "limit": 100, "opt_fields": "name,completed,due_on,assignee,projects,notes,created_at,modified_at"}
    
    if last_sync_at:
        params["modified_since"] = last_sync_at.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    all_tasks = []
    while True:
        data = _request("GET", url, token, params=params)
        tasks = data.get("data", [])
        all_tasks.extend(tasks)
        
        next_page = data.get("next_page")
        if not next_page or not next_page.get("uri"):
            break
        url = f"https://app.asana.com{next_page['uri']}"
        params = {}
    
    return all_tasks


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


def _get_token(cfg: dict) -> str | None:
    if not cfg:
        return None
    return (
        cfg.get("access_token")
        or cfg.get("api_token")
        or cfg.get("api_key")
        or cfg.get("api_secret")
    )


def connect_asana(uid: str) -> dict:
    cfg = _get_config(uid)
    token = _get_token(cfg)
    if not token:
        return {"status": "failed", "error": "Missing credentials"}

    try:
        response = requests.get(
            "https://app.asana.com/api/1.0/users/me",
            headers=_get_headers(token),
            timeout=10
        )
        if response.status_code >= 400:
            raise Exception(f"API Error {response.status_code}")
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "failed", "error": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid}")
    return {"status": "success"}


def sync_asana(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Asana not configured"}

    token = cfg["access_token"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        workspaces = _fetch_workspaces(token)
        if not workspaces:
            raise Exception("No workspaces found")
        
        workspace_gid = workspaces[0]["gid"]
        
        projects = _fetch_projects(token, workspace_gid)
        tasks = _fetch_tasks(token, workspace_gid, last_sync_at)
        
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Projects
    project_rows = []
    for project in projects:
        project_rows.append({
            "uid": uid,
            "source": PROJECTS_SOURCE,
            "project_gid": project.get("gid"),
            "name": project.get("name"),
            "archived": bool(project.get("archived")),
            "workspace_gid": workspace_gid,
            "created_at": project.get("created_at"),
            "modified_at": project.get("modified_at"),
            "data_json": json.dumps(project, default=str),
            "raw_json": json.dumps(project, default=str),
            "fetched_at": fetched_at,
        })

    # Process Tasks
    task_rows = []
    for task in tasks:
        assignee = task.get("assignee") or {}
        task_rows.append({
            "uid": uid,
            "source": TASKS_SOURCE,
            "task_gid": task.get("gid"),
            "name": task.get("name"),
            "completed": bool(task.get("completed")),
            "due_on": task.get("due_on"),
            "assignee_gid": assignee.get("gid"),
            "assignee_name": assignee.get("name"),
            "notes": task.get("notes"),
            "created_at": task.get("created_at"),
            "modified_at": task.get("modified_at"),
            "data_json": json.dumps(task, default=str),
            "raw_json": json.dumps(task, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(project_rows) + len(task_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PROJECTS_SOURCE, project_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, TASKS_SOURCE, task_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "projects_found": len(project_rows),
        "tasks_found": len(task_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_asana(uid: str) -> dict:
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
