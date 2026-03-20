import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "klaviyo"
PROFILES_SOURCE = "klaviyo_profiles"
EVENTS_SOURCE = "klaviyo_events"
LISTS_SOURCE = "klaviyo_lists"
API_BASE = "https://a.klaviyo.com/api"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[KLAVIYO] {message}", flush=True)


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


def _get_headers(api_key: str):
    return {
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "Accept": "application/json",
        "revision": "2024-10-15",
    }


def _request(method: str, path: str, api_key: str, retries: int = 4, **kwargs):
    url = f"{API_BASE}{path}"
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
        raise Exception(f"Klaviyo API error {response.status_code}: {detail}")

    return response.json()


def _fetch_paginated(path: str, api_key: str) -> list[dict]:
    items = []
    cursor = None

    while True:
        params = {"page[size]": 100}
        if cursor:
            params["page[cursor]"] = cursor

        data = _request("GET", path, api_key, params=params)
        
        batch = data.get("data") or []
        items.extend(batch)
        
        links = data.get("links") or {}
        next_link = links.get("next")
        if not next_link:
            break
        
        # Extract cursor from next link
        if "page%5Bcursor%5D=" in next_link:
            cursor = next_link.split("page%5Bcursor%5D=")[1].split("&")[0]
        else:
            break

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


def connect_klaviyo(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Klaviyo not configured for this user"}

    try:
        # Test API connection by fetching account info
        data = _request("GET", "/accounts", cfg["api_key"])
        accounts = data.get("data") or []
        account = accounts[0] if accounts else {}
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
        "api_key": _mask_token(cfg.get("api_key")),
        "account_id": account.get("id"),
    }


def sync_klaviyo(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Klaviyo not configured"}

    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        profiles = _fetch_paginated("/profiles", cfg["api_key"])
        events = _fetch_paginated("/events", cfg["api_key"])
        lists = _fetch_paginated("/lists", cfg["api_key"])
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    # Filter by updated for incremental sync
    if last_sync_at:
        profiles = [p for p in profiles if (_parse_dt((p.get("attributes") or {}).get("updated")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]
        events = [e for e in events if (_parse_dt((e.get("attributes") or {}).get("timestamp")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]
        # Lists don't have updated timestamps, so we fetch all for incremental

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    profile_rows = []
    for profile in profiles:
        attrs = profile.get("attributes") or {}
        profile_rows.append(
            {
                "uid": uid,
                "source": PROFILES_SOURCE,
                "profile_id": profile.get("id"),
                "email": attrs.get("email"),
                "phone_number": attrs.get("phone_number"),
                "first_name": attrs.get("first_name"),
                "last_name": attrs.get("last_name"),
                "created": attrs.get("created"),
                "updated": attrs.get("updated"),
                "data_json": json.dumps(profile, default=str),
                "raw_json": json.dumps(profile, default=str),
                "fetched_at": fetched_at,
            }
        )

    event_rows = []
    for event in events:
        attrs = event.get("attributes") or {}
        event_rows.append(
            {
                "uid": uid,
                "source": EVENTS_SOURCE,
                "event_id": event.get("id"),
                "event_type": attrs.get("metric_id"),
                "timestamp": attrs.get("timestamp"),
                "profile_id": ((event.get("relationships") or {}).get("profile") or {}).get("data", {}).get("id"),
                "data_json": json.dumps(event, default=str),
                "raw_json": json.dumps(event, default=str),
                "fetched_at": fetched_at,
            }
        )

    list_rows = []
    for list_item in lists:
        attrs = list_item.get("attributes") or {}
        list_rows.append(
            {
                "uid": uid,
                "source": LISTS_SOURCE,
                "list_id": list_item.get("id"),
                "name": attrs.get("name"),
                "created": attrs.get("created"),
                "updated": attrs.get("updated"),
                "data_json": json.dumps(list_item, default=str),
                "raw_json": json.dumps(list_item, default=str),
                "fetched_at": fetched_at,
            }
        )

    total_rows_found = len(profile_rows) + len(event_rows) + len(list_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PROFILES_SOURCE, profile_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, EVENTS_SOURCE, event_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, LISTS_SOURCE, list_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "profiles_found": len(profile_rows),
        "events_found": len(event_rows),
        "lists_found": len(list_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_klaviyo(uid: str) -> dict:
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
