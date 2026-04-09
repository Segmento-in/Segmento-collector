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
SOURCE = "okta"
USERS_SOURCE = "okta_users"
GROUPS_SOURCE = "okta_groups"
APPS_SOURCE = "okta_apps"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[OKTA] {message}", flush=True)


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * max(len(token) - 8, 4)}{token[-4:]}"


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


def save_config(uid: str, payload: dict):
    config = {
        "api_token": (payload.get("api_token") or "").strip(),
        "base_url": (payload.get("base_url") or "").strip().rstrip("/"),
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
    _log(f"Config saved for uid={uid}")


def _get_headers(token: str) -> dict:
    return {
        "Authorization": f"SSWS {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, token: str, retries: int = 3, **kwargs):
    headers = _get_headers(token)
    for attempt in range(retries):
        response = requests.request(method, url, headers=headers, timeout=30, **kwargs)
        if response.status_code == 429:
            wait_s = 2 ** attempt
            if attempt == retries - 1:
                break
            _log(f"Rate limited; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue
        if response.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                break
            wait_s = min(2 ** attempt, 15)
            time.sleep(wait_s)
            continue
        break
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Okta API error {response.status_code}: {detail}")
    return response.json()


def _paginate(token: str, url: str) -> list[dict]:
    results = []
    while url:
        response = requests.get(url, headers=_get_headers(token), timeout=30)
        if response.status_code >= 400:
            break
        results.extend(response.json() if isinstance(response.json(), list) else [])
        link_header = response.headers.get("Link", "")
        next_url = None
        for part in link_header.split(","):
            if 'rel="next"' in part:
                next_url = part.split(";")[0].strip().strip("<>")
                break
        url = next_url
    return results


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


def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg:
        _log(f"No active destination configured. Skipping push for {label}")
        return 0
    if not rows:
        _log(f"No rows generated for {label}. Skipping push")
        return 0
    _log(f"Pushing {len(rows)} rows ? {label}")
    pushed = push_to_destination(dest_cfg, route_source, rows)
    _log(f"Push complete ? {label}: {pushed} rows")
    return pushed


def connect_okta(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Okta not configured for this user"}

    base_url = cfg.get("base_url", "")
    token = cfg.get("api_token", "")

    try:
        data = _request("GET", f"{base_url}/api/v1/users?limit=1", token)
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
        "api_token": _mask_token(token),
        "base_url": base_url,
    }


def sync_okta(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Okta not configured"}

    base_url = cfg.get("base_url", "")
    token = cfg.get("api_token", "")
    fetched_at = _iso_now()

    try:
        raw_users = _paginate(token, f"{base_url}/api/v1/users?limit=200")
        raw_groups = _paginate(token, f"{base_url}/api/v1/groups?limit=200")
        raw_apps = _paginate(token, f"{base_url}/api/v1/apps?limit=200")
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)

    user_rows = [
        {
            "uid": uid,
            "source": USERS_SOURCE,
            "user_id": u.get("id"),
            "login": (u.get("profile") or {}).get("login"),
            "email": (u.get("profile") or {}).get("email"),
            "first_name": (u.get("profile") or {}).get("firstName"),
            "last_name": (u.get("profile") or {}).get("lastName"),
            "status": u.get("status"),
            "created": u.get("created"),
            "last_login": u.get("lastLogin"),
            "raw_json": json.dumps(u, default=str),
            "fetched_at": fetched_at,
        }
        for u in raw_users
    ]

    group_rows = [
        {
            "uid": uid,
            "source": GROUPS_SOURCE,
            "group_id": g.get("id"),
            "name": (g.get("profile") or {}).get("name"),
            "description": (g.get("profile") or {}).get("description"),
            "type": g.get("type"),
            "created": g.get("created"),
            "raw_json": json.dumps(g, default=str),
            "fetched_at": fetched_at,
        }
        for g in raw_groups
    ]

    app_rows = [
        {
            "uid": uid,
            "source": APPS_SOURCE,
            "app_id": a.get("id"),
            "name": a.get("name"),
            "label": a.get("label"),
            "status": a.get("status"),
            "sign_on_mode": a.get("signOnMode"),
            "created": a.get("created"),
            "raw_json": json.dumps(a, default=str),
            "fetched_at": fetched_at,
        }
        for a in raw_apps
    ]

    total_pushed = 0
    total_pushed += _push_rows(dest_cfg, SOURCE, USERS_SOURCE, user_rows)
    total_pushed += _push_rows(dest_cfg, SOURCE, GROUPS_SOURCE, group_rows)
    total_pushed += _push_rows(dest_cfg, SOURCE, APPS_SOURCE, app_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")

    result = {
        "status": "success",
        "users_found": len(user_rows),
        "groups_found": len(group_rows),
        "apps_found": len(app_rows),
        "rows_pushed": total_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_okta(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}

