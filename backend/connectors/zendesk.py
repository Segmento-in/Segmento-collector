import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "zendesk"
TICKETS_SOURCE = "zendesk_tickets"
USERS_SOURCE = "zendesk_users"
ORGANIZATIONS_SOURCE = "zendesk_organizations"

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con

def _log(message: str):
    print(f"[ZENDESK] {message}")

def _iso_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()

def _parse_dt(value):
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)
    except Exception:
        return None

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
        "INSERT OR REPLACE INTO connector_state (uid, source, state_json, updated_at) VALUES (?, ?, ?, ?)",
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

def save_config(uid: str, subdomain: str, email: str, api_token: str):
    config = {
        "subdomain": subdomain.strip(),
        "email": email.strip(),
        "api_token": api_token.strip(),
    }
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO connector_configs (uid, connector, config_json, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
        (uid, SOURCE, encrypt_value(json.dumps(config)), _iso_now()),
    )
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}")

def _request(method: str, path: str, cfg: dict, params=None, retries: int = 4):
    subdomain = cfg["subdomain"]
    email = cfg["email"]
    token = cfg["api_token"]
    url = f"https://{subdomain}.zendesk.com/api/v2{path}"
    auth = (f"{email}/token", token)

    for attempt in range(retries):
        try:
            res = requests.request(method, url, auth=auth, params=params, timeout=40)
            if res.status_code == 429:
                wait_s = int(res.headers.get("Retry-After", 2 ** attempt))
                _log(f"Rate limited; sleeping {wait_s}s")
                time.sleep(wait_s)
                continue
            if res.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            break
        except Exception as e:
            if attempt == retries - 1: raise
            time.sleep(2 ** attempt)

    if res.status_code >= 400:
        raise Exception(f"Zendesk API error {res.status_code}: {res.text[:200]}")
    return res.json()

def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg or not rows: return 0
    return push_to_destination(dest_cfg, route_source, rows)

def connect_zendesk(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg: return {"status": "error", "message": "Not configured"}
    try:
        _request("GET", "/users/me.json", cfg)
        _update_status(uid, "connected")
        _set_connection_enabled(uid, True)
        return {"status": "success", "subdomain": cfg["subdomain"]}
    except Exception as e:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(e)}

def sync_zendesk(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg: return {"status": "error", "message": "Not configured"}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"
    
    # Simple pagination: max 3 pages, 50 per page as per requirements
    def fetch_all(path, key):
        items = []
        url_path = path
        for _ in range(3):
            data = _request("GET", url_path, cfg, params={"per_page": 50})
            items.extend(data.get(key, []))
            url_path = data.get("next_page")
            if not url_path: break
            url_path = url_path.replace(f"https://{cfg['subdomain']}.zendesk.com/api/v2", "")
            time.sleep(0.5) # small delay
        return items

    try:
        results = {}
        # Tickets
        tickets = fetch_all("/tickets.json", "tickets")
        rows = []
        for t in tickets:
            rows.append({
                "uid": uid, "source": TICKETS_SOURCE, "ticket_id": t.get("id"),
                "subject": t.get("subject"), "status": t.get("status"),
                "created_at": t.get("created_at"), "updated_at": t.get("updated_at"),
                "data_json": json.dumps(t), "fetched_at": fetched_at
            })
        results["tickets_found"] = len(rows)
        results["tickets_pushed"] = _push_rows(dest_cfg, TICKETS_SOURCE, "tickets", rows)

        # Users
        users = fetch_all("/users.json", "users")
        rows = []
        for u in users:
            rows.append({
                "uid": uid, "source": USERS_SOURCE, "user_id": u.get("id"),
                "name": u.get("name"), "email": u.get("email"), "role": u.get("role"),
                "data_json": json.dumps(u), "fetched_at": fetched_at
            })
        results["users_found"] = len(rows)
        results["users_pushed"] = _push_rows(dest_cfg, USERS_SOURCE, "users", rows)

        # Organizations
        orgs = fetch_all("/organizations.json", "organizations")
        rows = []
        for o in orgs:
            rows.append({
                "uid": uid, "source": ORGANIZATIONS_SOURCE, "org_id": o.get("id"),
                "name": o.get("name"), "data_json": json.dumps(o), "fetched_at": fetched_at
            })
        results["orgs_found"] = len(rows)
        results["orgs_pushed"] = _push_rows(dest_cfg, ORGANIZATIONS_SOURCE, "organizations", rows)

        save_state(uid, {"last_sync_at": _iso_now()})
        return {"status": "success", **results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def disconnect_zendesk(uid: str) -> dict:
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
