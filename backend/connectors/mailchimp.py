import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "mailchimp"
LISTS_SOURCE = "mailchimp_lists"
MEMBERS_SOURCE = "mailchimp_members"
CAMPAIGNS_SOURCE = "mailchimp_campaigns"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def _log(message: str):
    print(f"[MAILCHIMP] {message}", flush=True)

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

def save_config(uid: str, api_key: str):
    config = {"api_key": api_key.strip()}
    con = get_db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO connector_configs (uid, connector, config_json, status, created_at) VALUES (?, ?, ?, 'pending', ?)", (uid, SOURCE, encrypt_value(json.dumps(config)), _iso_now()))
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}")

def _request(method: str, path: str, api_key: str, params=None, retries: int = 4):
    dc = "us1"
    if "-" in api_key:
        dc = api_key.split("-")[-1]
    url = f"https://{dc}.api.mailchimp.com/3.0{path}"
    
    for attempt in range(retries):
        try:
            res = requests.request(method, url, auth=("apikey", api_key), params=params, timeout=40)
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
        raise Exception(f"Mailchimp API error {res.status_code}: {res.text[:200]}")
    return res.json()

def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg or not rows: return 0
    return push_to_destination(dest_cfg, route_source, rows)

def connect_mailchimp(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg: return {"status": "error", "message": "Not configured"}
    try:
        _request("GET", "/", cfg["api_key"])
        _update_status(uid, "connected")
        _set_connection_enabled(uid, True)
        return {"status": "success"}
    except Exception as e:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(e)}

def sync_mailchimp(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg: return {"status": "error", "message": "Not configured"}
    api_key = cfg["api_key"]
    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    def fetch_all(path, key):
        items = []
        # Page size 50 as per requirements
        offset = 0
        for _ in range(3): # max 3 pages
            data = _request("GET", path, api_key, params={"count": 50, "offset": offset})
            batch = data.get(key, [])
            items.extend(batch)
            if len(batch) < 50: break
            offset += 50
            time.sleep(0.5)
        return items

    try:
        results = {}
        # Lists (Audiences)
        lists = fetch_all("/lists", "lists")
        rows = []
        for l in lists:
            rows.append({
                "uid": uid, "source": LISTS_SOURCE, "list_id": l.get("id"),
                "name": l.get("name"), "contact_email": (l.get("contact") or {}).get("email"),
                "member_count": (l.get("stats") or {}).get("member_count"),
                "data_json": json.dumps(l), "fetched_at": fetched_at
            })
            
            # Members for each list
            members = fetch_all(f"/lists/{l.get('id')}/members", "members")
            m_rows = []
            for m in members:
                m_rows.append({
                    "uid": uid, "source": MEMBERS_SOURCE, "member_id": m.get("id"),
                    "list_id": l.get("id"), "email": m.get("email_address"),
                    "status": m.get("status"), "data_json": json.dumps(m), "fetched_at": fetched_at
                })
            results[f"members_{l.get('id')}_pushed"] = _push_rows(dest_cfg, MEMBERS_SOURCE, "members", m_rows)

        results["lists_found"] = len(rows)
        results["lists_pushed"] = _push_rows(dest_cfg, LISTS_SOURCE, "lists", rows)

        # Campaigns
        campaigns = fetch_all("/campaigns", "campaigns")
        rows = []
        for c in campaigns:
            rows.append({
                "uid": uid, "source": CAMPAIGNS_SOURCE, "campaign_id": c.get("id"),
                "title": (c.get("settings") or {}).get("title"),
                "subject": (c.get("settings") or {}).get("subject_line"),
                "status": c.get("status"), "data_json": json.dumps(c), "fetched_at": fetched_at
            })
        results["campaigns_found"] = len(rows)
        results["campaigns_pushed"] = _push_rows(dest_cfg, CAMPAIGNS_SOURCE, "campaigns", rows)

        return {"status": "success", **results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def disconnect_mailchimp(uid: str) -> dict:
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
