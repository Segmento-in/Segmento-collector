import datetime
import json
import sqlite3
import time
import gzip
from io import BytesIO

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "amplitude"
EVENTS_SOURCE = "amplitude_events"
USERS_SOURCE = "amplitude_users"
SESSIONS_SOURCE = "amplitude_sessions"
API_BASE = "https://amplitude.com/api/2"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[AMPLITUDE] {message}", flush=True)


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


def save_config(uid: str, api_key: str, secret_key: str):
    config = {
        "api_key": api_key.strip(),
        "secret_key": secret_key.strip(),
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
    _log(f"Config saved for uid={uid}")


def _request(method: str, url: str, api_key: str, secret_key: str, retries: int = 4, **kwargs):
    for attempt in range(retries):
        response = requests.request(
            method, url, auth=(api_key, secret_key), timeout=60, **kwargs
        )

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
        raise Exception(f"Amplitude API error {response.status_code}: {detail}")

    return response


def _fetch_export_events(api_key: str, secret_key: str, start: str, end: str) -> list[dict]:
    """
    Fetch events from the Export API
    Events are returned as newline-delimited JSON (possibly gzipped)
    """
    url = f"{API_BASE}/export"
    params = {
        "start": start,
        "end": end,
    }
    
    response = _request("GET", url, api_key, secret_key, params=params)
    
    # Check if response is gzipped
    content = response.content
    if content[:2] == b'\x1f\x8b':  # gzip magic number
        with gzip.GzipFile(fileobj=BytesIO(content)) as f:
            content = f.read()
    
    # Parse newline-delimited JSON
    events = []
    for line in content.decode('utf-8').strip().split('\n'):
        if line:
            try:
                events.append(json.loads(line))
            except Exception as e:
                _log(f"Failed to parse event line: {e}")
    
    return events


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


def connect_amplitude(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Amplitude not configured for this user"}

    try:
        # Test API connection by fetching a small date range
        yesterday = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1)).strftime("%Y%m%d")
        today = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")
        _fetch_export_events(cfg["api_key"], cfg["secret_key"], yesterday, today)
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
        "secret_key": _mask_token(cfg.get("secret_key")),
    }


def sync_amplitude(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Amplitude not configured"}

    state = get_state(uid)
    
    # Determine date range
    if sync_type == "incremental" and state.get("last_sync_at"):
        last_sync = _parse_dt(state["last_sync_at"])
        start_date = last_sync.strftime("%Y%m%dT%H")
    else:
        # Default to last 7 days for historical sync
        start_date = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)).strftime("%Y%m%d")
    
    end_date = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H")

    try:
        events = _fetch_export_events(cfg["api_key"], cfg["secret_key"], start_date, end_date)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    # Extract events, users, and sessions from the raw event data
    event_rows = []
    user_map = {}
    session_map = {}

    for event in events:
        # Event row
        event_rows.append(
            {
                "uid": uid,
                "source": EVENTS_SOURCE,
                "event_id": event.get("event_id"),
                "event_type": event.get("event_type"),
                "user_id": event.get("user_id"),
                "amplitude_id": event.get("amplitude_id"),
                "device_id": event.get("device_id"),
                "session_id": event.get("session_id"),
                "event_time": event.get("event_time"),
                "server_upload_time": event.get("server_upload_time"),
                "platform": event.get("platform"),
                "os_name": event.get("os_name"),
                "device_type": event.get("device_type"),
                "country": event.get("country"),
                "city": event.get("city"),
                "data_json": json.dumps(event, default=str),
                "raw_json": json.dumps(event, default=str),
                "fetched_at": fetched_at,
            }
        )

        # Track unique users
        user_id = event.get("user_id") or event.get("amplitude_id")
        if user_id and user_id not in user_map:
            user_map[user_id] = {
                "uid": uid,
                "source": USERS_SOURCE,
                "user_id": user_id,
                "amplitude_id": event.get("amplitude_id"),
                "device_id": event.get("device_id"),
                "platform": event.get("platform"),
                "os_name": event.get("os_name"),
                "country": event.get("country"),
                "city": event.get("city"),
                "language": event.get("language"),
                "data_json": json.dumps(event, default=str),
                "raw_json": json.dumps(event, default=str),
                "fetched_at": fetched_at,
            }

        # Track unique sessions
        session_id = event.get("session_id")
        if session_id and session_id not in session_map:
            session_map[session_id] = {
                "uid": uid,
                "source": SESSIONS_SOURCE,
                "session_id": session_id,
                "user_id": user_id,
                "start_time": event.get("event_time"),
                "platform": event.get("platform"),
                "device_type": event.get("device_type"),
                "data_json": json.dumps(event, default=str),
                "raw_json": json.dumps(event, default=str),
                "fetched_at": fetched_at,
            }

    user_rows = list(user_map.values())
    session_rows = list(session_map.values())

    total_rows_found = len(event_rows) + len(user_rows) + len(session_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, EVENTS_SOURCE, event_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, USERS_SOURCE, user_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, SESSIONS_SOURCE, session_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "events_found": len(event_rows),
        "users_found": len(user_rows),
        "sessions_found": len(session_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_amplitude(uid: str) -> dict:
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
