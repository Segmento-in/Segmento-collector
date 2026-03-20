import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "twilio"
MESSAGES_SOURCE = "twilio_messages"
CALLS_SOURCE = "twilio_calls"
RECORDINGS_SOURCE = "twilio_recordings"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def _log(message: str):
    print(f"[TWILIO] {message}", flush=True)

def _iso_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()

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

def save_config(uid: str, account_sid: str, auth_token: str):
    config = {
        "account_sid": account_sid.strip(),
        "auth_token": auth_token.strip(),
    }
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_configs (uid, connector, config_json, status, created_at) VALUES (?, ?, ?, 'pending', ?)", (uid, SOURCE, encrypt_value(json.dumps(config)), _iso_now()))
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}")

def _request(method: str, path: str, cfg: dict, params=None, retries: int = 4):
    sid = cfg["account_sid"]
    token = cfg["auth_token"]
    url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}{path}"
    
    for attempt in range(retries):
        try:
            res = requests.request(method, url, auth=(sid, token), params=params, timeout=40)
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
        raise Exception(f"Twilio API error {res.status_code}: {res.text[:200]}")
    return res.json()

def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg or not rows: return 0
    return push_to_destination(dest_cfg, route_source, rows)

def connect_twilio(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg: return {"status": "error", "message": "Not configured"}
    try:
        _request("GET", ".json", cfg)
        _update_status(uid, "connected")
        _set_connection_enabled(uid, True)
        return {"status": "success"}
    except Exception as e:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(e)}

def sync_twilio(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg: return {"status": "error", "message": "Not configured"}
    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    def fetch_all(path, key):
        items = []
        url_path = path
        for _ in range(3): # max 3 pages
            data = _request("GET", url_path, cfg, params={"PageSize": 50})
            items.extend(data.get(key, []))
            next_page_uri = data.get("next_page_uri")
            if not next_page_uri: break
            # Twilio next_page_uri is relative to base, but we need to strip account part if redundant
            # But the _request already prepends base. Twilio URIs usually include /2010.../Accounts/{sid}/...
            # We need to extract the part after Accounts/{sid}
            url_path = next_page_uri.split(f"Accounts/{cfg['account_sid']}")[-1]
            time.sleep(0.5)
        return items

    try:
        results = {}
        # Messages
        messages = fetch_all("/Messages.json", "messages")
        rows = []
        for m in messages:
            rows.append({
                "uid": uid, "source": MESSAGES_SOURCE, "message_sid": m.get("sid"),
                "from": m.get("from"), "to": m.get("to"), "body": m.get("body"),
                "status": m.get("status"), "date_sent": m.get("date_sent"),
                "data_json": json.dumps(m), "fetched_at": fetched_at
            })
        results["messages_found"] = len(rows)
        results["messages_pushed"] = _push_rows(dest_cfg, MESSAGES_SOURCE, "messages", rows)

        # Calls
        calls = fetch_all("/Calls.json", "calls")
        rows = []
        for c in calls:
            rows.append({
                "uid": uid, "source": CALLS_SOURCE, "call_sid": c.get("sid"),
                "from": c.get("from"), "to": c.get("to"), "duration": c.get("duration"),
                "status": c.get("status"), "start_time": c.get("start_time"),
                "data_json": json.dumps(c), "fetched_at": fetched_at
            })
        results["calls_found"] = len(rows)
        results["calls_pushed"] = _push_rows(dest_cfg, CALLS_SOURCE, "calls", rows)

        # Recordings
        recordings = fetch_all("/Recordings.json", "recordings")
        rows = []
        for r in recordings:
            rows.append({
                "uid": uid, "source": RECORDINGS_SOURCE, "recording_sid": r.get("sid"),
                "call_sid": r.get("call_sid"), "duration": r.get("duration"),
                "status": r.get("status"), "date_created": r.get("date_created"),
                "data_json": json.dumps(r), "fetched_at": fetched_at
            })
        results["recordings_found"] = len(rows)
        results["recordings_pushed"] = _push_rows(dest_cfg, RECORDINGS_SOURCE, "recordings", rows)

        save_state(uid, {"last_sync_at": _iso_now()})
        return {"status": "success", **results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def disconnect_twilio(uid: str) -> dict:
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
