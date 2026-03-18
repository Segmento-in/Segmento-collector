import datetime
import json
import sqlite3
import time
from base64 import b64encode

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "mixpanel"
EVENTS_SOURCE = "mixpanel_events"
USERS_SOURCE = "mixpanel_users"
API_BASE = "https://data.mixpanel.com/api"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[MIXPANEL] {message}")


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


def save_config(uid: str, api_secret: str):
    config = {"api_secret": api_secret.strip()}

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


def _get_auth_header(api_secret: str):
    """Create basic auth header from API secret"""
    # Mixpanel uses the API secret as username with empty password
    auth_string = f"{api_secret}:"
    encoded = b64encode(auth_string.encode()).decode()
    return f"Basic {encoded}"


def _request(method: str, url: str, api_secret: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers["Authorization"] = _get_auth_header(api_secret)
    headers["Accept"] = "application/json"

    for attempt in range(retries):
        response = requests.request(method, url, headers=headers, timeout=40, **kwargs)

        if response.status_code == 429:
            wait_s = min(2 ** attempt, 15)
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
        raise Exception(f"Mixpanel API error {response.status_code}: {detail}")

    return response.json() if response.text else {}


def _fetch_events(api_secret: str, last_sync_at: datetime.datetime | None = None) -> list[dict]:
    """Fetch events from Mixpanel"""
    url = f"{API_BASE}/2.0/export"
    
    # Default to last 7 days if no last sync
    end_date = datetime.datetime.now(datetime.UTC)
    start_date = last_sync_at if last_sync_at else (end_date - datetime.timedelta(days=7))
    
    params = {
        "from_date": start_date.strftime("%Y-%m-%d"),
        "to_date": end_date.strftime("%Y-%m-%d"),
    }
    
    try:
        # Mixpanel export returns newline-delimited JSON
        response = requests.get(url, headers={"Authorization": _get_auth_header(api_secret)}, 
                              params=params, timeout=60)
        
        if response.status_code >= 400:
            raise Exception(f"API error {response.status_code}: {response.text}")
        
        events = []
        for line in response.text.strip().split('\n'):
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        
        return events
    except Exception as e:
        _log(f"Event fetch failed: {e}")
        return []


def _fetch_users(api_secret: str) -> list[dict]:
    """Fetch user profiles (engage API)"""
    url = f"{API_BASE}/2.0/engage"
    
    params = {
        "page_size": 1000,
    }
    
    try:
        data = _request("GET", url, api_secret, params=params)
        
        # Extract user profiles from response
        results = data.get("results", [])
        users = []
        for result in results:
            profile = result.get("$properties", {})
            profile["distinct_id"] = result.get("$distinct_id")
            users.append(profile)
        
        return users
    except Exception as e:
        _log(f"User fetch failed: {e}")
        return []


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


def connect_mixpanel(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Mixpanel not configured for this user"}

    try:
        # Test connection by fetching a small user sample
        users = _fetch_users(cfg["api_secret"])
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
        "user_count": len(users),
    }


def sync_mixpanel(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Mixpanel not configured"}

    api_secret = cfg["api_secret"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        events = _fetch_events(api_secret, last_sync_at)
        users = _fetch_users(api_secret)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Events
    event_rows = []
    for event in events:
        properties = event.get("properties", {})
        event_rows.append({
            "uid": uid,
            "source": EVENTS_SOURCE,
            "event": event.get("event"),
            "distinct_id": properties.get("distinct_id"),
            "time": properties.get("time"),
            "insert_id": properties.get("$insert_id"),
            "city": properties.get("$city"),
            "region": properties.get("$region"),
            "device": properties.get("$device"),
            "os": properties.get("$os"),
            "browser": properties.get("$browser"),
            "data_json": json.dumps(event, default=str),
            "raw_json": json.dumps(event, default=str),
            "fetched_at": fetched_at,
        })

    # Process Users
    user_rows = []
    for user in users:
        user_rows.append({
            "uid": uid,
            "source": USERS_SOURCE,
            "distinct_id": user.get("distinct_id"),
            "email": user.get("$email"),
            "name": user.get("$name"),
            "created": user.get("$created"),
            "last_seen": user.get("$last_seen"),
            "city": user.get("$city"),
            "region": user.get("$region"),
            "country_code": user.get("$country_code"),
            "data_json": json.dumps(user, default=str),
            "raw_json": json.dumps(user, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(event_rows) + len(user_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, EVENTS_SOURCE, event_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, USERS_SOURCE, user_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "events_found": len(event_rows),
        "users_found": len(user_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_mixpanel(uid: str) -> dict:
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
