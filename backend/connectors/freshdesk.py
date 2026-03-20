import datetime
import json
import sqlite3
import time
import base64

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "freshdesk"
TICKETS_SOURCE = "freshdesk_tickets"
CONTACTS_SOURCE = "freshdesk_contacts"
COMPANIES_SOURCE = "freshdesk_companies"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[FRESHDESK] {message}", flush=True)


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


def save_config(uid: str, domain: str, api_key: str):
    config = {
        "domain": domain.strip(),
        "api_key": api_key.strip(),
    }

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
    _log(f"Config saved for uid={uid} domain={config['domain']}")


def _get_headers(api_key: str):
    auth_str = f"{api_key}:X"
    encoded = base64.b64encode(auth_str.encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, api_key: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_get_headers(api_key))

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
        raise Exception(f"Freshdesk API error {response.status_code}: {detail}")

    return response.json()


def _fetch_paginated(domain: str, path: str, api_key: str) -> list[dict]:
    items = []
    page = 1
    per_page = 100

    while True:
        url = f"https://{domain}.freshdesk.com/api/v2{path}"
        data = _request("GET", url, api_key, params={"page": page, "per_page": per_page})
        
        if not data:
            break
        
        items.extend(data)
        
        if len(data) < per_page:
            break
        
        page += 1

    return items


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


def connect_freshdesk(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Freshdesk not configured for this user"}

    try:
        # Test API connection by fetching tickets
        url = f"https://{cfg['domain']}.freshdesk.com/api/v2/tickets"
        _request("GET", url, cfg["api_key"], params={"per_page": 1})
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} domain={cfg['domain']}")
    return {
        "status": "success",
        "api_key": _mask_token(cfg.get("api_key")),
        "domain": cfg["domain"],
    }


def sync_freshdesk(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Freshdesk not configured"}

    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        tickets = _fetch_paginated(cfg["domain"], "/tickets", cfg["api_key"])
        contacts = _fetch_paginated(cfg["domain"], "/contacts", cfg["api_key"])
        companies = _fetch_paginated(cfg["domain"], "/companies", cfg["api_key"])
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    # Filter by updated_at for incremental sync
    if last_sync_at:
        tickets = [t for t in tickets if (_parse_dt(t.get("updated_at")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]
        contacts = [c for c in contacts if (_parse_dt(c.get("updated_at")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]
        companies = [c for c in companies if (_parse_dt(c.get("updated_at")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    ticket_rows = []
    for ticket in tickets:
        ticket_rows.append(
            {
                "uid": uid,
                "source": TICKETS_SOURCE,
                "ticket_id": ticket.get("id"),
                "subject": ticket.get("subject"),
                "description": ticket.get("description"),
                "status": ticket.get("status"),
                "priority": ticket.get("priority"),
                "type": ticket.get("type"),
                "requester_id": ticket.get("requester_id"),
                "responder_id": ticket.get("responder_id"),
                "company_id": ticket.get("company_id"),
                "created_at": ticket.get("created_at"),
                "updated_at": ticket.get("updated_at"),
                "due_by": ticket.get("due_by"),
                "fr_due_by": ticket.get("fr_due_by"),
                "data_json": json.dumps(ticket, default=str),
                "raw_json": json.dumps(ticket, default=str),
                "fetched_at": fetched_at,
            }
        )

    contact_rows = []
    for contact in contacts:
        contact_rows.append(
            {
                "uid": uid,
                "source": CONTACTS_SOURCE,
                "contact_id": contact.get("id"),
                "name": contact.get("name"),
                "email": contact.get("email"),
                "phone": contact.get("phone"),
                "mobile": contact.get("mobile"),
                "company_id": contact.get("company_id"),
                "created_at": contact.get("created_at"),
                "updated_at": contact.get("updated_at"),
                "data_json": json.dumps(contact, default=str),
                "raw_json": json.dumps(contact, default=str),
                "fetched_at": fetched_at,
            }
        )

    company_rows = []
    for company in companies:
        company_rows.append(
            {
                "uid": uid,
                "source": COMPANIES_SOURCE,
                "company_id": company.get("id"),
                "name": company.get("name"),
                "description": company.get("description"),
                "domains": json.dumps(company.get("domains") or [], default=str),
                "created_at": company.get("created_at"),
                "updated_at": company.get("updated_at"),
                "data_json": json.dumps(company, default=str),
                "raw_json": json.dumps(company, default=str),
                "fetched_at": fetched_at,
            }
        )

    total_rows_found = len(ticket_rows) + len(contact_rows) + len(company_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, TICKETS_SOURCE, ticket_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, CONTACTS_SOURCE, contact_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, COMPANIES_SOURCE, company_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "tickets_found": len(ticket_rows),
        "contacts_found": len(contact_rows),
        "companies_found": len(company_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
        "domain": cfg["domain"],
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_freshdesk(uid: str) -> dict:
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
