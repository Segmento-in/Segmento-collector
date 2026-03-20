import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "notion"
PAGES_SOURCE = "notion_pages"
DATABASES_SOURCE = "notion_databases"
BLOCKS_SOURCE = "notion_blocks"
API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[NOTION] {message}", flush=True)


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


def save_config(uid: str, access_token: str):
    config = {"access_token": access_token.strip()}

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
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, token: str, retries: int = 4, **kwargs):
    url = f"{API_BASE}{path}"
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
        raise Exception(f"Notion API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


def _fetch_user(token: str) -> dict:
    return _request("GET", "/users/me", token)


def _fetch_search_objects(token: str, filter_value: str | None = None) -> list[dict]:
    results = []
    next_cursor = None

    while True:
        payload = {"page_size": 100, "sort": {"direction": "descending", "timestamp": "last_edited_time"}}
        if filter_value:
            payload["filter"] = {"value": filter_value, "property": "object"}
        if next_cursor:
            payload["start_cursor"] = next_cursor

        data = _request("POST", "/search", token, json=payload)
        results.extend(data.get("results", []))

        if not data.get("has_more") or not data.get("next_cursor"):
            break
        next_cursor = data.get("next_cursor")

    return results


def _fetch_block_children(token: str, block_id: str) -> list[dict]:
    results = []
    next_cursor = None

    while True:
        path = f"/blocks/{block_id}/children"
        params = {"page_size": 100}
        if next_cursor:
            params["start_cursor"] = next_cursor

        data = _request("GET", path, token, params=params)
        results.extend(data.get("results", []))

        if not data.get("has_more") or not data.get("next_cursor"):
            break
        next_cursor = data.get("next_cursor")

    return results


def _normalize_title(items) -> str | None:
    if not items:
        return None
    parts = []
    for item in items:
        text = ((item or {}).get("plain_text") or "").strip()
        if text:
            parts.append(text)
    return "".join(parts) if parts else None


def _page_title(page: dict) -> str | None:
    props = page.get("properties") or {}
    for prop in props.values():
        if prop.get("type") == "title":
            return _normalize_title(prop.get("title"))
    return None


def _database_title(database: dict) -> str | None:
    return _normalize_title(database.get("title"))


def _block_label(block: dict) -> str | None:
    block_type = block.get("type")
    block_data = (block.get(block_type) or {}) if block_type else {}
    if isinstance(block_data, dict):
        rich_text = block_data.get("rich_text")
        title = _normalize_title(rich_text)
        if title:
            return title
    return block_type


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


def connect_notion(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Notion not configured for this user"}

    try:
        me = _fetch_user(cfg["access_token"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    bot_name = me.get("name") or "Notion integration"
    workspace_name = ((me.get("bot") or {}).get("workspace_name"))

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} workspace={workspace_name or 'unknown'} bot={bot_name}")
    return {
        "status": "success",
        "access_token": _mask_token(cfg.get("access_token")),
        "bot_name": bot_name,
        "workspace_name": workspace_name,
    }


def sync_notion(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Notion not configured"}

    token = cfg["access_token"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        pages = _fetch_search_objects(token, "page")
        databases = _fetch_search_objects(token, "database")
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    if last_sync_at:
        pages = [p for p in pages if (_parse_dt(p.get("last_edited_time")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]
        databases = [d for d in databases if (_parse_dt(d.get("last_edited_time")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at]

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0
    block_rows = []

    page_rows = []
    for page in pages:
        page_id = page.get("id")
        page_rows.append(
            {
                "uid": uid,
                "source": PAGES_SOURCE,
                "page_id": page_id,
                "parent_id": ((page.get("parent") or {}).get("page_id") or (page.get("parent") or {}).get("database_id")),
                "title": _page_title(page),
                "url": page.get("url"),
                "created_time": page.get("created_time"),
                "last_edited_time": page.get("last_edited_time"),
                "archived": bool(page.get("archived")),
                "in_trash": bool(page.get("in_trash")),
                "data_json": json.dumps(page, default=str),
                "raw_json": json.dumps(page, default=str),
                "fetched_at": fetched_at,
            }
        )

        if page_id:
            try:
                blocks = _fetch_block_children(token, page_id)
            except Exception as exc:
                _log(f"Failed to fetch blocks for page {page_id}: {exc}")
                blocks = []

            for block in blocks:
                block_rows.append(
                    {
                        "uid": uid,
                        "source": BLOCKS_SOURCE,
                        "block_id": block.get("id"),
                        "parent_id": page_id,
                        "parent_type": "page",
                        "block_type": block.get("type"),
                        "label": _block_label(block),
                        "created_time": block.get("created_time"),
                        "last_edited_time": block.get("last_edited_time"),
                        "has_children": bool(block.get("has_children")),
                        "archived": bool(block.get("archived")),
                        "in_trash": bool(block.get("in_trash")),
                        "data_json": json.dumps(block, default=str),
                        "raw_json": json.dumps(block, default=str),
                        "fetched_at": fetched_at,
                    }
                )

    database_rows = []
    for database in databases:
        database_id = database.get("id")
        database_rows.append(
            {
                "uid": uid,
                "source": DATABASES_SOURCE,
                "database_id": database_id,
                "parent_id": ((database.get("parent") or {}).get("page_id")),
                "title": _database_title(database),
                "url": database.get("url"),
                "created_time": database.get("created_time"),
                "last_edited_time": database.get("last_edited_time"),
                "archived": bool(database.get("archived")),
                "in_trash": bool(database.get("in_trash")),
                "data_json": json.dumps(database, default=str),
                "raw_json": json.dumps(database, default=str),
                "fetched_at": fetched_at,
            }
        )

        if database_id:
            try:
                blocks = _fetch_block_children(token, database_id)
            except Exception as exc:
                _log(f"Failed to fetch blocks for database {database_id}: {exc}")
                blocks = []

            for block in blocks:
                block_rows.append(
                    {
                        "uid": uid,
                        "source": BLOCKS_SOURCE,
                        "block_id": block.get("id"),
                        "parent_id": database_id,
                        "parent_type": "database",
                        "block_type": block.get("type"),
                        "label": _block_label(block),
                        "created_time": block.get("created_time"),
                        "last_edited_time": block.get("last_edited_time"),
                        "has_children": bool(block.get("has_children")),
                        "archived": bool(block.get("archived")),
                        "in_trash": bool(block.get("in_trash")),
                        "data_json": json.dumps(block, default=str),
                        "raw_json": json.dumps(block, default=str),
                        "fetched_at": fetched_at,
                    }
                )

    total_rows_found += len(page_rows) + len(database_rows) + len(block_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PAGES_SOURCE, page_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, DATABASES_SOURCE, database_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, BLOCKS_SOURCE, block_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "pages_found": len(page_rows),
        "databases_found": len(database_rows),
        "blocks_found": len(block_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_notion(uid: str) -> dict:
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
