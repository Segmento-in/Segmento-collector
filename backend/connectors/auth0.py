import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "auth0"
USERS_SOURCE = "auth0_users"
ROLES_SOURCE = "auth0_roles"
LOGS_SOURCE = "auth0_logs"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[AUTH0] {message}", flush=True)


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * max(len(token) - 8, 4)}{token[-4:]}"


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
        "access_token": (payload.get("access_token") or "").strip(),
        "domain": (payload.get("domain") or "").strip().rstrip("/"),
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
        "Authorization": f"Bearer {token}",
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
        raise Exception(f"Auth0 API error {response.status_code}: {detail}")
    return response.json()


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
    _log(f"Pushing {len(rows)} rows → {label}")
    pushed = push_to_destination(dest_cfg, route_source, rows)
    _log(f"Push complete → {label}: {pushed} rows")
    return pushed


def connect_auth0(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Auth0 not configured for this user"}

    domain = cfg.get("domain", "")
    token = cfg.get("access_token", "")
    base_url = f"https://{domain}/api/v2"

    try:
        _request("GET", f"{base_url}/users?per_page=1", token)
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} domain={domain}")
    return {
        "status": "success",
        "access_token": _mask_token(token),
        "domain": domain,
    }


def sync_auth0(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Auth0 not configured"}

    domain = cfg.get("domain", "")
    token = cfg.get("access_token", "")
    base_url = f"https://{domain}/api/v2"
    fetched_at = _iso_now()

    try:
        raw_users = _request("GET", f"{base_url}/users?per_page=100", token)
        raw_roles = _request("GET", f"{base_url}/roles?per_page=100", token)
        raw_logs = _request("GET", f"{base_url}/logs?per_page=100", token)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)

    if not isinstance(raw_users, list):
        raw_users = raw_users.get("users", []) if isinstance(raw_users, dict) else []
    if not isinstance(raw_roles, list):
        raw_roles = raw_roles.get("roles", []) if isinstance(raw_roles, dict) else []
    if not isinstance(raw_logs, list):
        raw_logs = raw_logs.get("logs", []) if isinstance(raw_logs, dict) else []

    user_rows = [
        {
            "uid": uid,
            "source": USERS_SOURCE,
            "user_id": u.get("user_id"),
            "email": u.get("email"),
            "name": u.get("name"),
            "nickname": u.get("nickname"),
            "picture": u.get("picture"),
            "created_at": u.get("created_at"),
            "updated_at": u.get("updated_at"),
            "last_login": u.get("last_login"),
            "logins_count": u.get("logins_count"),
            "raw_json": json.dumps(u, default=str),
            "fetched_at": fetched_at,
        }
        for u in raw_users
    ]

    role_rows = [
        {
            "uid": uid,
            "source": ROLES_SOURCE,
            "role_id": r.get("id"),
            "name": r.get("name"),
            "description": r.get("description"),
            "raw_json": json.dumps(r, default=str),
            "fetched_at": fetched_at,
        }
        for r in raw_roles
    ]

    log_rows = [
        {
            "uid": uid,
            "source": LOGS_SOURCE,
            "log_id": l.get("log_id") or l.get("_id"),
            "type": l.get("type"),
            "date": l.get("date"),
            "user_id": l.get("user_id"),
            "user_name": l.get("user_name"),
            "ip": l.get("ip"),
            "description": l.get("description"),
            "raw_json": json.dumps(l, default=str),
            "fetched_at": fetched_at,
        }
        for l in raw_logs
    ]

    total_pushed = 0
    total_pushed += _push_rows(dest_cfg, SOURCE, USERS_SOURCE, user_rows)
    total_pushed += _push_rows(dest_cfg, SOURCE, ROLES_SOURCE, role_rows)
    total_pushed += _push_rows(dest_cfg, SOURCE, LOGS_SOURCE, log_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")

    result = {
        "status": "success",
        "users_found": len(user_rows),
        "roles_found": len(role_rows),
        "logs_found": len(log_rows),
        "rows_pushed": total_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_auth0(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}
