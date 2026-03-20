import json
import sqlite3
import datetime
import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "azure_blob"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def _log(message: str):
    print(f"[AZURE_BLOB] {message}", flush=True)

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

def connect_azure_blob(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Azure Blob Storage not configured for this user"}

    connection_string = cfg.get("connection_string")
    container_name = cfg.get("container_name")
    
    # Simple check if credentials exist
    if not connection_string or not container_name:
        return {"status": "error", "message": "Missing required details"}
        
    try:
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        # Just to verify auth
        if not container_client.exists():
            raise Exception("Container doesn't exist or no permission.")
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid}")
    return {"status": "success", "message": "Connected successfully"}

def fetch_azure_blobs(connection_string, container_name):
    from azure.storage.blob import BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    blob_list = container_client.list_blobs()
    results = []
    for blob in blob_list:
        results.append({
            "name": blob.name,
            "container": container_name,
            "size": blob.size,
            "content_type": blob.content_settings.content_type,
            "last_modified": str(blob.last_modified),
        })
    return results

def sync_azure_blob(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Azure Blob not configured"}

    connection_string = cfg.get("connection_string")
    container_name = cfg.get("container_name")

    try:
        items = fetch_azure_blobs(connection_string, container_name)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    table_name = "azure_blob_files"
    
    rows = []
    for item in items:
        rows.append({
            "uid": uid,
            "source": table_name,
            "blob_name": item["name"],
            "container_name": item["container"],
            "size": item["size"],
            "last_modified": item["last_modified"],
            "data_json": json.dumps(item, default=str),
            "raw_json": json.dumps(item, default=str),
            "fetched_at": fetched_at,
        })
        
    total_rows_found = len(rows)
    total_rows_pushed = 0
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

def disconnect_azure_blob(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}
