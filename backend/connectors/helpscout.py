import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "helpscout"
CONVERSATIONS_SOURCE = "helpscout_conversations"
CUSTOMERS_SOURCE = "helpscout_customers"
MAILBOXES_SOURCE = "helpscout_mailboxes"
API_BASE = "https://api.helpscout.net/v2"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[HELPSCOUT] {message}", flush=True)


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * max(len(token) - 8, 4)}{token[-4:]}"


def _parse_dt(value):
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)
    except Exception:
        return None


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


def get_state(uid: str) -> dict:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source=?
        LIMIT 1
        """,
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


def save_config(uid: str, api_key: str):
    config = {"api_key": api_key.strip()}

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


def _get_headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, token: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_get_headers(token))

    for attempt in range(retries):
        response = requests.request(method, url, headers=headers, timeout=40, **kwargs)

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            try:
                wait_s = int(retry_after) if retry_after else 2 ** attempt
            except Exception:
                wait_s = 2 ** attempt
            if attempt == retries - 1:
                break
            _log(f"Rate limited on {url}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if response.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                break
            wait_s = min(2 ** attempt, 15)
            _log(f"Server error {response.status_code} on {url}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        break

    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"HelpScout API error {response.status_code}: {detail}")

    return response.json() if response.text else {}


def _fetch_mailboxes(token: str):
    """Fetch all mailboxes"""
    url = f"{API_BASE}/mailboxes"
    data = _request("GET", url, token)
    
    mailboxes = []
    if "_embedded" in data and "mailboxes" in data["_embedded"]:
        mailboxes = data["_embedded"]["mailboxes"]
    
    return mailboxes


def _fetch_conversations(token: str, mailbox_id: str, last_sync_at: datetime.datetime | None = None):
    """Fetch conversations from a mailbox"""
    url = f"{API_BASE}/conversations"
    params = {"mailbox": mailbox_id, "status": "all"}
    
    if last_sync_at:
        params["modifiedSince"] = last_sync_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    data = _request("GET", url, token, params=params)
    
    conversations = []
    if "_embedded" in data and "conversations" in data["_embedded"]:
        conversations = data["_embedded"]["conversations"]
    
    return conversations


def _fetch_customers(token: str):
    """Fetch customers"""
    url = f"{API_BASE}/customers"
    params = {"page": 1}
    
    all_customers = []
    
    while True:
        data = _request("GET", url, token, params=params)
        
        if "_embedded" in data and "customers" in data["_embedded"]:
            customers = data["_embedded"]["customers"]
            all_customers.extend(customers)
        
        # Check for next page
        page_info = data.get("page", {})
        if page_info.get("number", 1) >= page_info.get("totalPages", 1):
            break
        
        params["page"] += 1
        
        # Limit to first 10 pages to avoid long sync times
        if params["page"] > 10:
            _log("Limiting customer fetch to 10 pages")
            break
    
    return all_customers


def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg:
        _log(f"No active destination configured. Skipping push for {label}")
        return 0
    if not rows:
        _log(f"No rows generated for {label}. Skipping push")
        return 0

    _log(
        f"Pushing {len(rows)} rows to destination "
        f"(route_source={route_source}, label={label}, dest_type={dest_cfg.get('type')})"
    )
    pushed = push_to_destination(dest_cfg, route_source, rows)
    _log(
        f"Destination push completed "
        f"(route_source={route_source}, label={label}, rows_pushed={pushed})"
    )
    return pushed


def _get_token(cfg: dict) -> str | None:
    if not cfg:
        return None
    return (
        cfg.get("access_token")
        or cfg.get("api_token")
        or cfg.get("api_key")
        or cfg.get("api_secret")
    )


def connect_helpscout(uid: str) -> dict:
    cfg = _get_config(uid)
    token = _get_token(cfg)
    if not token:
        return {"status": "failed", "error": "Missing credentials"}

    import base64
    auth = base64.b64encode(f"{token}:X".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(
            "https://api.helpscout.net/v2/users",
            headers=headers,
            timeout=10
        )
        if response.status_code >= 400:
            raise Exception(f"API Error {response.status_code}")
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "failed", "error": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid}")
    return {"status": "success"}


def sync_helpscout(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "HelpScout not configured"}

    token = cfg["api_key"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        mailboxes = _fetch_mailboxes(token)
        customers = _fetch_customers(token)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Mailboxes
    mailbox_rows = []
    for mailbox in mailboxes:
        mailbox_rows.append({
            "uid": uid,
            "source": MAILBOXES_SOURCE,
            "mailbox_id": str(mailbox.get("id")),
            "name": mailbox.get("name"),
            "email": mailbox.get("email"),
            "created_at": mailbox.get("createdAt"),
            "updated_at": mailbox.get("updatedAt"),
            "data_json": json.dumps(mailbox, default=str),
            "raw_json": json.dumps(mailbox, default=str),
            "fetched_at": fetched_at,
        })

    # Process Conversations
    conversation_rows = []
    for mailbox in mailboxes:
        try:
            conversations = _fetch_conversations(token, str(mailbox["id"]), last_sync_at)
            
            for conv in conversations:
                conversation_rows.append({
                    "uid": uid,
                    "source": CONVERSATIONS_SOURCE,
                    "conversation_id": str(conv.get("id")),
                    "mailbox_id": str(mailbox["id"]),
                    "number": conv.get("number"),
                    "subject": conv.get("subject"),
                    "status": conv.get("status"),
                    "type": conv.get("type"),
                    "created_at": conv.get("createdAt"),
                    "updated_at": conv.get("userUpdatedAt"),
                    "customer_id": str(conv.get("primaryCustomer", {}).get("id")) if conv.get("primaryCustomer") else None,
                    "data_json": json.dumps(conv, default=str),
                    "raw_json": json.dumps(conv, default=str),
                    "fetched_at": fetched_at,
                })
        except Exception as e:
            _log(f"Failed to fetch conversations for mailbox {mailbox['id']}: {e}")
            continue

    # Process Customers
    customer_rows = []
    for customer in customers:
        customer_rows.append({
            "uid": uid,
            "source": CUSTOMERS_SOURCE,
            "customer_id": str(customer.get("id")),
            "first_name": customer.get("firstName"),
            "last_name": customer.get("lastName"),
            "email": customer.get("emails", [{}])[0].get("value") if customer.get("emails") else None,
            "created_at": customer.get("createdAt"),
            "updated_at": customer.get("updatedAt"),
            "data_json": json.dumps(customer, default=str),
            "raw_json": json.dumps(customer, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(mailbox_rows) + len(conversation_rows) + len(customer_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, MAILBOXES_SOURCE, mailbox_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, CONVERSATIONS_SOURCE, conversation_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, CUSTOMERS_SOURCE, customer_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "mailboxes_found": len(mailbox_rows),
        "conversations_found": len(conversation_rows),
        "customers_found": len(customer_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_helpscout(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
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
