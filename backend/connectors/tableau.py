import datetime
import json
import sqlite3
import os

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = os.getenv("DB_PATH", "/tmp/identity.db")
SOURCE = "tableau"
WORKBOOKS_SOURCE = "tableau_workbooks"
DATASOURCES_SOURCE = "tableau_datasources"
PROJECTS_SOURCE = "tableau_projects"
API_VERSION = "3.19"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _iso_now():
    return datetime.datetime.utcnow().isoformat()


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


def _normalize_base_url(base_url: str) -> str:
    url = (base_url or "").strip().rstrip("/")
    if not url:
        return ""
    if "/api/" not in url:
        url = f"{url}/api/{API_VERSION}"
    return url


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * max(len(token) - 8, 4)}{token[-4:]}"


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
        "token_name": (payload.get("token_name") or "").strip(),
        "token_secret": (payload.get("token_secret") or "").strip(),
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


def _request(method: str, url: str, *, headers=None, json_payload=None, params=None):
    response = requests.request(
        method,
        url,
        headers=headers,
        json=json_payload,
        params=params,
        timeout=10,
    )
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Tableau API error {response.status_code}: {detail}")
    if not response.text:
        return {}
    return response.json()


def _signin(cfg: dict) -> dict:
    return _request(
        "POST",
        f"{cfg['base_url']}/auth/signin",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json_payload={
            "credentials": {
                "personalAccessTokenName": cfg["token_name"],
                "personalAccessTokenSecret": cfg["token_secret"],
                "site": {"contentUrl": ""},
            }
        },
    )


def _tableau_headers(token: str) -> dict:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Tableau-Auth": token,
    }


def _fetch_collection(cfg: dict, token: str, site_id: str, key: str) -> list[dict]:
    data = _request(
        "GET",
        f"{cfg['base_url']}/sites/{site_id}/{key}",
        headers=_tableau_headers(token),
        params={"pageSize": 1000},
    )
    return ((data.get(key) or {}).get(key[:-1]) or [])


def _filter_rows(rows: list[dict], last_sync_at, field_name: str):
    if not last_sync_at:
        return rows
    filtered = []
    for row in rows:
        updated_at = _parse_dt(row.get(field_name))
        if not updated_at or updated_at > last_sync_at:
            filtered.append(row)
    return filtered


def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg or not rows:
        return 0
    return push_to_destination(dest_cfg, route_source, rows)


def connect_tableau(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Tableau not configured"}
    try:
        auth = _signin(cfg)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}
    credentials = auth.get("credentials") or {}
    site = credentials.get("site") or {}
    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    return {
        "status": "success",
        "token_name": cfg.get("token_name"),
        "token_secret": _mask_token(cfg.get("token_secret")),
        "site_id": site.get("id"),
        "site_name": site.get("contentUrl") or "default",
        "user_id": (credentials.get("user") or {}).get("id"),
    }


def sync_tableau(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Tableau not configured"}
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None
    try:
        auth = _signin(cfg)
        credentials = auth.get("credentials") or {}
        token = credentials.get("token")
        site_id = (credentials.get("site") or {}).get("id")
        if not token or not site_id:
            raise Exception("Missing Tableau auth token or site id")
        workbooks = _fetch_collection(cfg, token, site_id, "workbooks")
        datasources = _fetch_collection(cfg, token, site_id, "datasources")
        projects = _fetch_collection(cfg, token, site_id, "projects")
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    workbooks = _filter_rows(workbooks, last_sync_at, "updatedAt")
    datasources = _filter_rows(datasources, last_sync_at, "updatedAt")
    projects = _filter_rows(projects, last_sync_at, "updatedAt")

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    workbook_rows = [{
        "uid": uid,
        "source": WORKBOOKS_SOURCE,
        "workbook_id": item.get("id"),
        "name": item.get("name"),
        "project_id": (item.get("project") or {}).get("id"),
        "project_name": (item.get("project") or {}).get("name"),
        "owner_id": (item.get("owner") or {}).get("id"),
        "content_url": item.get("contentUrl"),
        "webpage_url": item.get("webpageUrl"),
        "created_at": item.get("createdAt"),
        "updated_at": item.get("updatedAt"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in workbooks]

    datasource_rows = [{
        "uid": uid,
        "source": DATASOURCES_SOURCE,
        "datasource_id": item.get("id"),
        "name": item.get("name"),
        "project_id": (item.get("project") or {}).get("id"),
        "project_name": (item.get("project") or {}).get("name"),
        "owner_id": (item.get("owner") or {}).get("id"),
        "content_url": item.get("contentUrl"),
        "created_at": item.get("createdAt"),
        "updated_at": item.get("updatedAt"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in datasources]

    project_rows = [{
        "uid": uid,
        "source": PROJECTS_SOURCE,
        "project_id": item.get("id"),
        "name": item.get("name"),
        "description": item.get("description"),
        "parent_project_id": (item.get("parentProject") or {}).get("id"),
        "controlling_permissions_project_id": (item.get("controllingPermissionsProject") or {}).get("id"),
        "top_level_project": item.get("topLevelProject"),
        "created_at": item.get("createdAt"),
        "updated_at": item.get("updatedAt"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in projects]

    rows_found = len(workbook_rows) + len(datasource_rows) + len(project_rows)
    rows_pushed = 0
    rows_pushed += _push_rows(dest_cfg, SOURCE, WORKBOOKS_SOURCE, workbook_rows)
    rows_pushed += _push_rows(dest_cfg, SOURCE, DATASOURCES_SOURCE, datasource_rows)
    rows_pushed += _push_rows(dest_cfg, SOURCE, PROJECTS_SOURCE, project_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "workbooks_found": len(workbook_rows),
        "datasources_found": len(datasource_rows),
        "projects_found": len(project_rows),
        "rows_found": rows_found,
        "rows_pushed": rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_tableau(uid: str) -> dict:
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

