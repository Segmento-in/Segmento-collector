import json
import sqlite3
import datetime
import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "datadog"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def _log(message: str):
    print(f"[DATADOG] {message}", flush=True)

def _iso_now():
    return datetime.datetime.now(datetime.UTC).isoformat()

def _get_config(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector=? LIMIT 1", (uid, SOURCE))
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
    cur.execute("SELECT state_json FROM connector_state WHERE uid=? AND source=? LIMIT 1", (uid, SOURCE))
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
        "INSERT OR REPLACE INTO connector_state (uid, source, state_json, updated_at) VALUES (?, ?, ?, ?)",
        (uid, SOURCE, json.dumps(state), _iso_now()),
    )
    con.commit()
    con.close()

def _update_status(uid: str, status: str):
    con = get_db()
    cur = con.cursor()
    cur.execute("UPDATE connector_configs SET status=? WHERE uid=? AND connector=?", (status, uid, SOURCE))
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
        "INSERT OR REPLACE INTO connector_configs (uid, connector, config_json, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
        (uid, SOURCE, encrypt_value(json.dumps(config)), _iso_now()),
    )
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}")

def _get_active_destination(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT dest_type, host, port, username, password, database_name FROM destination_configs WHERE uid=? AND source=? AND is_active=1 LIMIT 1",
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
        return 0
    if not rows:
        return 0
    pushed = push_to_destination(dest_cfg, route_source, rows)
    return pushed

def connect_datadog(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Datadog not configured for this user"}

    api_key = cfg.get("api_key")
    app_key = cfg.get("application_key")

    try:
        url = "https://api.datadoghq.com/api/v1/validate"
        headers = {
            "DD-API-KEY": api_key,
            "DD-APPLICATION-KEY": app_key
        }
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid}")
    return {"status": "success", "message": "Connected successfully"}

def fetch_datadog_objects(api_key, app_key, endpoint):
    url = f"https://api.datadoghq.com/api/v1/{endpoint}"
    headers = {
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key
    }
    # Time based filtering fallback logic can be mapped here; for now, simple get
    try:
        if endpoint == 'events':
            import time
            end_time = int(time.time())
            start_time = end_time - 86400
            url = f"{url}?start={start_time}&end={end_time}"
        res = requests.get(url, headers=headers, timeout=30)
        res.raise_for_status()
        data = res.json()
        if isinstance(data, dict) and "events" in data:
            return data["events"]
        elif isinstance(data, dict) and "logs" in data:
            return data["logs"]
        elif isinstance(data, dict) and "metrics" in data:
            return data["metrics"]
        elif isinstance(data, list):
            return data
        return []
    except Exception:
        return []

def sync_datadog(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Datadog not configured"}

    api_key = cfg.get("api_key")
    app_key = cfg.get("application_key")

    # validate connection first
    try:
        url = "https://api.datadoghq.com/api/v1/validate"
        res = requests.get(url, headers={"DD-API-KEY": api_key, "DD-APPLICATION-KEY": app_key}, timeout=10)
        res.raise_for_status()
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    objects_map = {
        "events": "datadog_events",
        #"logs": "datadog_logs", # Not easily fetchable without specific query via v1/logs-queries
        #"metrics": "datadog_metrics" 
    }

    total_rows_found = 0
    total_rows_pushed = 0

    for endpoint, table_name in objects_map.items():
        try:
            items = fetch_datadog_objects(api_key, app_key, endpoint)
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

def disconnect_datadog(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}
