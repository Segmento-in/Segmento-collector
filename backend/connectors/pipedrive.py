import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "pipedrive"
DEALS_SOURCE = "pipedrive_deals"
PERSONS_SOURCE = "pipedrive_persons"
ORGANIZATIONS_SOURCE = "pipedrive_organizations"
API_BASE = "https://api.pipedrive.com/v1"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[PIPEDRIVE] {message}")


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


def save_config(uid: str, api_token: str):
    config = {"api_token": api_token.strip()}

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


def _request(method: str, path: str, api_token: str, retries: int = 4, **kwargs):
    url = f"{API_BASE}{path}"
    params = dict(kwargs.pop("params", {}) or {})
    params["api_token"] = api_token

    for attempt in range(retries):
        response = requests.request(method, url, params=params, timeout=40, **kwargs)

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            try:
                wait_s = int(retry_after) if retry_after else 2 ** attempt
            except Exception:
                wait_s = 2 ** attempt
            if attempt == retries - 1:
                break
            _log(f"Rate limited on {path}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if response.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                break
            wait_s = min(2 ** attempt, 15)
            _log(f"Server error {response.status_code} on {path}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        break

    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Pipedrive API error {response.status_code}: {detail}")

    return response.json()


def _fetch_paginated(path: str, api_token: str) -> list[dict]:
    items = []
    start = 0
    limit = 100

    while True:
        data = _request("GET", path, api_token, params={"start": start, "limit": limit})
        
        batch = data.get("data") or []
        if not batch:
            break
        
        items.extend(batch)
        
        additional_data = data.get("additional_data") or {}
        pagination = additional_data.get("pagination") or {}
        if not pagination.get("more_items_in_collection"):
            break
        
        start = pagination.get("next_start", start + limit)

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


def connect_pipedrive(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Pipedrive not configured for this user"}

    try:
        # Test API connection by fetching user info
        data = _request("GET", "/users/me", cfg["api_token"])
        user = data.get("data") or {}
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} user={user.get('name', 'unknown')}")
    return {
        "status": "success",
        "api_token": _mask_token(cfg.get("api_token")),
        "user_name": user.get("name"),
        "company_name": user.get("company_name"),
    }


def sync_pipedrive(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Pipedrive not configured"}

    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        deals = _fetch_paginated("/deals", cfg["api_token"])
        persons = _fetch_paginated("/persons", cfg["api_token"])
        organizations = _fetch_paginated("/organizations", cfg["api_token"])
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    # Filter by update_time for incremental sync
    if last_sync_at:
        deals = [d for d in deals if (_parse_dt(d.get("update_time")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]
        persons = [p for p in persons if (_parse_dt(p.get("update_time")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]
        organizations = [o for o in organizations if (_parse_dt(o.get("update_time")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    deal_rows = []
    for deal in deals:
        deal_rows.append(
            {
                "uid": uid,
                "source": DEALS_SOURCE,
                "deal_id": deal.get("id"),
                "title": deal.get("title"),
                "value": deal.get("value"),
                "currency": deal.get("currency"),
                "status": deal.get("status"),
                "stage_id": deal.get("stage_id"),
                "person_id": deal.get("person_id") and deal["person_id"].get("value"),
                "org_id": deal.get("org_id") and deal["org_id"].get("value"),
                "add_time": deal.get("add_time"),
                "update_time": deal.get("update_time"),
                "close_time": deal.get("close_time"),
                "won_time": deal.get("won_time"),
                "lost_time": deal.get("lost_time"),
                "data_json": json.dumps(deal, default=str),
                "raw_json": json.dumps(deal, default=str),
                "fetched_at": fetched_at,
            }
        )

    person_rows = []
    for person in persons:
        person_rows.append(
            {
                "uid": uid,
                "source": PERSONS_SOURCE,
                "person_id": person.get("id"),
                "name": person.get("name"),
                "email": (person.get("email") or [{}])[0].get("value") if person.get("email") else None,
                "phone": (person.get("phone") or [{}])[0].get("value") if person.get("phone") else None,
                "org_id": person.get("org_id") and person["org_id"].get("value"),
                "owner_id": person.get("owner_id") and person["owner_id"].get("value"),
                "add_time": person.get("add_time"),
                "update_time": person.get("update_time"),
                "data_json": json.dumps(person, default=str),
                "raw_json": json.dumps(person, default=str),
                "fetched_at": fetched_at,
            }
        )

    org_rows = []
    for org in organizations:
        org_rows.append(
            {
                "uid": uid,
                "source": ORGANIZATIONS_SOURCE,
                "org_id": org.get("id"),
                "name": org.get("name"),
                "address": org.get("address"),
                "owner_id": org.get("owner_id") and org["owner_id"].get("value"),
                "people_count": org.get("people_count"),
                "add_time": org.get("add_time"),
                "update_time": org.get("update_time"),
                "data_json": json.dumps(org, default=str),
                "raw_json": json.dumps(org, default=str),
                "fetched_at": fetched_at,
            }
        )

    total_rows_found = len(deal_rows) + len(person_rows) + len(org_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, DEALS_SOURCE, deal_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PERSONS_SOURCE, person_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, ORGANIZATIONS_SOURCE, org_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "deals_found": len(deal_rows),
        "persons_found": len(person_rows),
        "organizations_found": len(org_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_pipedrive(uid: str) -> dict:
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
