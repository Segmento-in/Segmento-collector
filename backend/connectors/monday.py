import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "monday"
BOARDS_SOURCE = "monday_boards"
ITEMS_SOURCE = "monday_items"
USERS_SOURCE = "monday_users"
API_BASE = "https://api.monday.com/v2"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[MONDAY] {message}", flush=True)


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


def _get_headers(token: str):
    return {
        "Authorization": token,
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
        raise Exception(f"Monday.com API error {response.status_code}: {detail}")

    return response.json() if response.text else {}


def _graphql_query(token: str, query: str, variables: dict = None):
    """Execute GraphQL query against Monday.com API"""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    
    return _request("POST", API_BASE, token, json=payload)


def _fetch_boards(token: str):
    """Fetch all boards"""
    query = """
    {
        boards {
            id
            name
            description
            state
            board_kind
            created_at
            updated_at
        }
    }
    """
    data = _graphql_query(token, query)
    return data.get("data", {}).get("boards", [])


def _fetch_items(token: str, board_id: str, last_sync_at: datetime.datetime | None = None):
    """Fetch items from a board"""
    query = """
    query ($boardId: ID!) {
        boards(ids: [$boardId]) {
            items_page {
                items {
                    id
                    name
                    state
                    created_at
                    updated_at
                    creator_id
                    column_values {
                        id
                        text
                        value
                    }
                }
            }
        }
    }
    """
    
    data = _graphql_query(token, query, {"boardId": board_id})
    boards = data.get("data", {}).get("boards", [])
    if not boards:
        return []
    
    items = boards[0].get("items_page", {}).get("items", [])
    
    # Filter by last_sync_at if provided
    if last_sync_at:
        filtered = []
        for item in items:
            updated_at = _parse_dt(item.get("updated_at"))
            if updated_at and updated_at > last_sync_at:
                filtered.append(item)
        return filtered
    
    return items


def _fetch_users(token: str):
    """Fetch all users"""
    query = """
    {
        users {
            id
            name
            email
            created_at
            is_admin
            is_guest
        }
    }
    """
    data = _graphql_query(token, query)
    return data.get("data", {}).get("users", [])


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


def connect_monday(uid: str) -> dict:
    cfg = _get_config(uid)
    token = _get_token(cfg)
    if not token:
        return {"status": "failed", "error": "Missing credentials"}

    try:
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        response = requests.post(
            "https://api.monday.com/v2",
            headers=headers,
            json={"query": "{ me { id name } }"},
            timeout=10
        )
        if response.status_code >= 400:
            raise Exception(f"API Error {response.status_code}")
        
        data = response.json()
        if "errors" in data:
            raise Exception(f"GraphQL Error: {data['errors']}")
            
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "failed", "error": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid}")
    return {"status": "success"}


def sync_monday(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Monday.com not configured"}

    token = cfg["api_token"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        boards = _fetch_boards(token)
        users = _fetch_users(token)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Boards
    board_rows = []
    for board in boards:
        board_rows.append({
            "uid": uid,
            "source": BOARDS_SOURCE,
            "board_id": board.get("id"),
            "name": board.get("name"),
            "description": board.get("description"),
            "state": board.get("state"),
            "board_kind": board.get("board_kind"),
            "created_at": board.get("created_at"),
            "updated_at": board.get("updated_at"),
            "data_json": json.dumps(board, default=str),
            "raw_json": json.dumps(board, default=str),
            "fetched_at": fetched_at,
        })

    # Process Items (from all boards)
    item_rows = []
    for board in boards:
        try:
            items = _fetch_items(token, board["id"], last_sync_at)
            for item in items:
                item_rows.append({
                    "uid": uid,
                    "source": ITEMS_SOURCE,
                    "item_id": item.get("id"),
                    "board_id": board["id"],
                    "name": item.get("name"),
                    "state": item.get("state"),
                    "creator_id": item.get("creator_id"),
                    "created_at": item.get("created_at"),
                    "updated_at": item.get("updated_at"),
                    "column_values": json.dumps(item.get("column_values", []), default=str),
                    "data_json": json.dumps(item, default=str),
                    "raw_json": json.dumps(item, default=str),
                    "fetched_at": fetched_at,
                })
        except Exception as e:
            _log(f"Failed to fetch items for board {board['id']}: {e}")
            continue

    # Process Users
    user_rows = []
    for user in users:
        user_rows.append({
            "uid": uid,
            "source": USERS_SOURCE,
            "user_id": user.get("id"),
            "name": user.get("name"),
            "email": user.get("email"),
            "created_at": user.get("created_at"),
            "is_admin": bool(user.get("is_admin")),
            "is_guest": bool(user.get("is_guest")),
            "data_json": json.dumps(user, default=str),
            "raw_json": json.dumps(user, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(board_rows) + len(item_rows) + len(user_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, BOARDS_SOURCE, board_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, ITEMS_SOURCE, item_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, USERS_SOURCE, user_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "boards_found": len(board_rows),
        "items_found": len(item_rows),
        "users_found": len(user_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_monday(uid: str) -> dict:
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
