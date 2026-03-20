import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "intercom"
CONTACTS_SOURCE = "intercom_contacts"
CONVERSATIONS_SOURCE = "intercom_conversations"
COMPANIES_SOURCE = "intercom_companies"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def _log(message: str):
    print(f"[INTERCOM] {message}", flush=True)

def _iso_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()

def _parse_dt(value):
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            dt = datetime.datetime.fromtimestamp(value, datetime.UTC)
        else:
            text = str(value).strip()
            if text.endswith("Z"): text = text.replace("Z", "+00:00")
            dt = datetime.datetime.fromisoformat(text)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)
    except Exception:
        return None

def _mask_token(token: str | None) -> str | None:
    if not token: return None
    if len(token) <= 8: return "*" * len(token)
    return f"{token[:4]}{'*' * max(len(token) - 8, 4)}{token[-4:]}"

def _get_config(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT config_json FROM connector_configs WHERE uid=? AND connector=? LIMIT 1", (uid, SOURCE))
    row = fetchone_secure(cur)
    con.close()
    if not row or not row.get("config_json"): return None
    try: return json.loads(row["config_json"])
    except Exception: return None

def get_state(uid: str) -> dict:
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT state_json FROM connector_state WHERE uid=? AND source=? LIMIT 1", (uid, SOURCE))
    row = fetchone_secure(cur)
    con.close()
    if not row or not row.get("state_json"): return {"last_sync_at": None}
    try: return json.loads(row["state_json"])
    except Exception: return {"last_sync_at": None}

def save_state(uid: str, state: dict):
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_state (uid, source, state_json, updated_at) VALUES (?, ?, ?, ?)", (uid, SOURCE, json.dumps(state), _iso_now()))
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
    cur.execute("UPDATE google_connections SET enabled=? WHERE uid=? AND source=?", (1 if enabled else 0, uid, SOURCE))
    if cur.rowcount == 0:
        cur.execute("INSERT INTO google_connections (uid, source, enabled) VALUES (?, ?, ?)", (uid, SOURCE, 1 if enabled else 0))
    con.commit()
    con.close()

def save_config(uid: str, access_token: str):
    config = {"access_token": access_token.strip()}
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_configs (uid, connector, config_json, status, created_at) VALUES (?, ?, ?, 'pending', ?)", (uid, SOURCE, encrypt_value(json.dumps(config)), _iso_now()))
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}")

def _request(method: str, path: str, token: str, params=None, retries: int = 4):
    url = f"https://api.intercom.io{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Intercom-Version": "2.0"
    }
    for attempt in range(retries):
        try:
            res = requests.request(method, url, headers=headers, params=params, timeout=40)
            if res.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if res.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            break
        except Exception:
            if attempt == retries - 1: raise
            time.sleep(2 ** attempt)
    if res.status_code >= 400:
        raise Exception(f"Intercom API error {res.status_code}: {res.text[:200]}")
    return res.json()

def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg or not rows: return 0
    return push_to_destination(dest_cfg, route_source, rows)

def connect_intercom(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg: return {"status": "error", "message": "Not configured"}
    try:
        _request("GET", "/me", cfg["access_token"])
        _update_status(uid, "connected")
        _set_connection_enabled(uid, True)
        return {"status": "success"}
    except Exception as e:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(e)}

def sync_intercom(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg: return {"status": "error", "message": "Not configured"}
    token = cfg["access_token"]
    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    def fetch_all(path, key):
        items = []
        data = _request("GET", path, token, params={"per_page": 50})
        items.extend(data.get(key, []))
        # Simple page limit for requirements (max 3 pages)
        for _ in range(2):
            pages = data.get("pages", {})
            next_url = pages.get("next")
            if not next_url: break
            # Intercom next_url might be absolute
            path_only = next_url.replace("https://api.intercom.io", "")
            data = _request("GET", path_only, token)
            items.extend(data.get(key, []))
            time.sleep(0.5)
        return items

    try:
        results = {}
        # Contacts
        contacts = fetch_all("/contacts", "data")
        rows = []
        for c in contacts:
            rows.append({
                "uid": uid, "source": CONTACTS_SOURCE, "contact_id": c.get("id"),
                "email": c.get("email"), "name": c.get("name"),
                "created_at": c.get("created_at"), "updated_at": c.get("updated_at"),
                "data_json": json.dumps(c), "fetched_at": fetched_at
            })
        results["contacts_found"] = len(rows)
        results["contacts_pushed"] = _push_rows(dest_cfg, CONTACTS_SOURCE, "contacts", rows)

        # Conversations
        convs = fetch_all("/conversations", "conversations")
        rows = []
        for cv in convs:
            rows.append({
                "uid": uid, "source": CONVERSATIONS_SOURCE, "conversation_id": cv.get("id"),
                "created_at": cv.get("created_at"), "updated_at": cv.get("updated_at"),
                "data_json": json.dumps(cv), "fetched_at": fetched_at
            })
        results["conversations_found"] = len(rows)
        results["conversations_pushed"] = _push_rows(dest_cfg, CONVERSATIONS_SOURCE, "conversations", rows)

        # Companies
        companies = fetch_all("/companies", "data")
        rows = []
        for cp in companies:
            rows.append({
                "uid": uid, "source": COMPANIES_SOURCE, "company_id": cp.get("id"),
                "name": cp.get("name"), "website": cp.get("website"),
                "data_json": json.dumps(cp), "fetched_at": fetched_at
            })
        results["companies_found"] = len(rows)
        results["companies_pushed"] = _push_rows(dest_cfg, COMPANIES_SOURCE, "companies", rows)

        save_state(uid, {"last_sync_at": _iso_now()})
        return {"status": "success", **results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def disconnect_intercom(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    return {"status": "disconnected"}

def _get_active_destination(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute("SELECT dest_type, host, port, username, password, database_name FROM destination_configs WHERE uid=? AND source=? AND is_active=1 LIMIT 1", (uid, SOURCE))
    row = fetchone_secure(cur)
    con.close()
    if not row: return None
    return {"type": row["dest_type"], "host": row["host"], "port": row["port"], "username": row["username"], "password": row["password"], "database_name": row["database_name"]}
