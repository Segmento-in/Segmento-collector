import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "sendgrid"
MESSAGES_SOURCE = "sendgrid_messages"
STATS_SOURCE = "sendgrid_stats"
API_BASE = "https://api.sendgrid.com/v3"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[SENDGRID] {message}", flush=True)


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
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, api_key: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_get_headers(api_key))

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
        raise Exception(f"SendGrid API error {response.status_code}: {detail}")

    return response.json() if response.text else {}


def _fetch_messages(api_key: str, last_sync_at: datetime.datetime | None = None) -> list[dict]:
    """Fetch email activity (messages)"""
    url = f"{API_BASE}/messages"
    
    # SendGrid requires specific query params for message retrieval
    query_params = {"limit": 1000}
    
    if last_sync_at:
        # Filter messages from last sync
        query_params["start_time"] = int(last_sync_at.timestamp())
    
    try:
        data = _request("GET", url, api_key, params=query_params)
        return data.get("messages", [])
    except Exception as e:
        _log(f"Message fetch not available or failed: {e}")
        return []


def _fetch_stats(api_key: str) -> list[dict]:
    """Fetch email statistics"""
    url = f"{API_BASE}/stats"
    
    # Get stats for last 30 days
    end_date = datetime.datetime.now(datetime.UTC)
    start_date = end_date - datetime.timedelta(days=30)
    
    params = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "aggregated_by": "day",
    }
    
    try:
        data = _request("GET", url, api_key, params=params)
        return data if isinstance(data, list) else data.get("stats", [])
    except Exception as e:
        _log(f"Stats fetch failed: {e}")
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


def _get_token(cfg: dict) -> str | None:
    if not cfg:
        return None
    return (
        cfg.get("access_token")
        or cfg.get("api_token")
        or cfg.get("api_key")
        or cfg.get("api_secret")
    )


def connect_sendgrid(uid: str) -> dict:
    cfg = _get_config(uid)
    token = _get_token(cfg)
    if not token:
        return {"status": "failed", "error": "Missing credentials"}

    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        response = requests.get(
            "https://api.sendgrid.com/v3/user/account",
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


def sync_sendgrid(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "SendGrid not configured"}

    api_key = cfg["api_key"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        messages = _fetch_messages(api_key, last_sync_at)
        stats = _fetch_stats(api_key)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Messages
    message_rows = []
    for msg in messages:
        message_rows.append({
            "uid": uid,
            "source": MESSAGES_SOURCE,
            "msg_id": msg.get("msg_id"),
            "from_email": msg.get("from_email"),
            "subject": msg.get("subject"),
            "to_email": msg.get("to_email"),
            "status": msg.get("status"),
            "opens_count": msg.get("opens_count", 0),
            "clicks_count": msg.get("clicks_count", 0),
            "last_event_time": msg.get("last_event_time"),
            "data_json": json.dumps(msg, default=str),
            "raw_json": json.dumps(msg, default=str),
            "fetched_at": fetched_at,
        })

    # Process Stats
    stat_rows = []
    for stat in stats:
        stat_rows.append({
            "uid": uid,
            "source": STATS_SOURCE,
            "date": stat.get("date"),
            "stats_type": stat.get("type", "global"),
            "requests": stat.get("stats", {}).get("requests", 0),
            "delivered": stat.get("stats", {}).get("delivered", 0),
            "opens": stat.get("stats", {}).get("opens", 0),
            "clicks": stat.get("stats", {}).get("clicks", 0),
            "bounces": stat.get("stats", {}).get("bounces", 0),
            "spam_reports": stat.get("stats", {}).get("spam_reports", 0),
            "data_json": json.dumps(stat, default=str),
            "raw_json": json.dumps(stat, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(message_rows) + len(stat_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, MESSAGES_SOURCE, message_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, STATS_SOURCE, stat_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "messages_found": len(message_rows),
        "stats_found": len(stat_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_sendgrid(uid: str) -> dict:
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
