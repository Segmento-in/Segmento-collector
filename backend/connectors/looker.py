import json
import sqlite3
import os
import time
import datetime
import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = os.getenv("DB_PATH", "identity.db")
SOURCE = "looker"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def _log(message: str):
    print(f"[LOOKER] {message}", flush=True)

def _iso_now():
    return datetime.datetime.utcnow().isoformat()

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

def save_config(uid: str, config: dict):
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

    _log(f"Pushing {len(rows)} rows to destination (route_source={route_source}, label={label}, dest_type={dest_cfg.get('type')})")
    pushed = push_to_destination(dest_cfg, route_source, rows)
    _log(f"Destination push completed (route_source={route_source}, label={label}, rows_pushed={pushed})")
    return pushed

def _get_access_token(base_url, client_id, client_secret):
    url = f"{base_url}/login"
    params = {"client_id": client_id, "client_secret": client_secret}
    res = requests.post(url, params=params, timeout=10)
    res.raise_for_status()
    return res.json().get("access_token")

def connect_looker(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Looker not configured for this user"}

    base_url = cfg.get("base_url", "").rstrip('/')
    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")

    try:
        token = _get_access_token(base_url, client_id, client_secret)
        if not token:
            raise Exception("Invalid credentials or Base URL")
        
        # Verify by fetching exactly 1 user
        res = requests.get(f"{base_url}/users", headers={"Authorization": f"Bearer {token}"}, params={"limit": 1})
        res.raise_for_status()
        
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
        "message": "Connected successfully"
    }

def fetch_looker_objects(token, base_url, endpoint):
    results = []
    limit = 100
    offset = 0
    while True:
        url = f"{base_url}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}"}
        params = {"limit": limit, "offset": offset}
        res = requests.get(url, headers=headers, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()
        if not isinstance(data, list):
            data = [data]
        if not data:
            break
        results.extend(data)
        if len(data) < limit:
            break
        offset += limit
    return results

def sync_looker(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Looker not configured"}

    base_url = cfg.get("base_url", "").rstrip('/')
    client_id = cfg.get("client_id")
    client_secret = cfg.get("client_secret")

    try:
        token = _get_access_token(base_url, client_id, client_secret)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    # Entities
    objects_map = {
        "users": "looker_users",
        "dashboards": "looker_dashboards",
        "looks": "looker_looks"
    }

    total_rows_found = 0
    total_rows_pushed = 0

    for endpoint, table_name in objects_map.items():
        try:
            items = fetch_looker_objects(token, base_url, endpoint)
        except Exception as e:
            _log(f"Failed to fetch {endpoint} for {uid}: {e}")
            items = []
            
        rows = []
        for item in items:
            rows.append({
                "uid": uid,
                "source": table_name,
                "item_id": str(item.get("id", "")),
                "data_json": json.dumps(item, default=str),
                "raw_json": json.dumps(item, default=str),
                "fetched_at": fetched_at,
            })
            
        total_rows_found += len(rows)
        if rows:
            total_rows_pushed += _push_rows(dest_cfg, SOURCE, table_name, rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result

def disconnect_looker(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}

