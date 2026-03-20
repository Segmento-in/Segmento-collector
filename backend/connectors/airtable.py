import datetime
import json
import sqlite3
import time
from urllib.parse import quote

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "airtable"
TABLES_SOURCE = "airtable_tables"
RECORDS_SOURCE = "airtable_records"
API_BASE = "https://api.airtable.com/v0"
META_API_BASE = "https://api.airtable.com/v0/meta"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[AIRTABLE] {message}", flush=True)


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


def save_config(uid: str, access_token: str, base_id: str, table_name: str):
    config = {
        "access_token": access_token.strip(),
        "base_id": base_id.strip(),
        "table_name": table_name.strip(),
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
    _log(f"Config saved for uid={uid} base={config['base_id']} table={config['table_name']}")


def _headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, token: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_headers(token))

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
        raise Exception(f"Airtable API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


def _fetch_base_tables(token: str, base_id: str) -> list[dict]:
    data = _request("GET", f"{META_API_BASE}/bases/{base_id}/tables", token)
    return data.get("tables", [])


def _fetch_records(token: str, base_id: str, table_name: str, formula: str | None = None) -> list[dict]:
    records = []
    offset = None
    encoded_table = quote(table_name, safe="")

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        if formula:
            params["filterByFormula"] = formula

        data = _request("GET", f"{API_BASE}/{base_id}/{encoded_table}", token, params=params)
        records.extend(data.get("records", []))

        offset = data.get("offset")
        if not offset:
            break

    return records


def _formula_since(last_sync_at):
    if not last_sync_at:
        return None
    dt = _parse_dt(last_sync_at)
    if not dt:
        return None
    # Airtable accepts ISO-like strings in formulas.
    return f"IS_AFTER(LAST_MODIFIED_TIME(), '{dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}')"


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


def connect_airtable(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Airtable not configured for this user"}

    try:
        tables = _fetch_base_tables(cfg["access_token"], cfg["base_id"])
        table_names = {table.get("name") for table in tables}
        if cfg["table_name"] not in table_names:
            raise Exception(
                f"Configured table '{cfg['table_name']}' was not found in base '{cfg['base_id']}'"
            )
        records = _fetch_records(cfg["access_token"], cfg["base_id"], cfg["table_name"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} base={cfg['base_id']} table={cfg['table_name']}")
    return {
        "status": "success",
        "access_token": _mask_token(cfg.get("access_token")),
        "base_id": cfg["base_id"],
        "table_name": cfg["table_name"],
        "tables_visible": len(tables),
        "records_visible": len(records),
    }


def sync_airtable(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Airtable not configured"}

    state = get_state(uid)
    formula = _formula_since(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        tables = _fetch_base_tables(cfg["access_token"], cfg["base_id"])
        try:
            records = _fetch_records(cfg["access_token"], cfg["base_id"], cfg["table_name"], formula=formula)
        except Exception as exc:
            if formula:
                _log(f"Incremental formula failed, falling back to full fetch: {exc}")
                records = _fetch_records(cfg["access_token"], cfg["base_id"], cfg["table_name"])
            else:
                raise
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    table_rows = []
    for table in tables:
        table_rows.append(
            {
                "uid": uid,
                "source": TABLES_SOURCE,
                "base_id": cfg["base_id"],
                "table_id": table.get("id"),
                "table_name": table.get("name"),
                "primary_field_id": table.get("primaryFieldId"),
                "view_count": len(table.get("views") or []),
                "field_count": len(table.get("fields") or []),
                "data_json": json.dumps(table, default=str),
                "raw_json": json.dumps(table, default=str),
                "fetched_at": fetched_at,
            }
        )

    record_rows = []
    for record in records:
        fields = record.get("fields") or {}
        record_rows.append(
            {
                "uid": uid,
                "source": RECORDS_SOURCE,
                "base_id": cfg["base_id"],
                "table_name": cfg["table_name"],
                "record_id": record.get("id"),
                "created_time": record.get("createdTime"),
                "field_count": len(fields),
                "fields_json": json.dumps(fields, default=str),
                "data_json": json.dumps(record, default=str),
                "raw_json": json.dumps(record, default=str),
                "fetched_at": fetched_at,
            }
        )

    total_rows_found = len(table_rows) + len(record_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, TABLES_SOURCE, table_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, RECORDS_SOURCE, record_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "tables_found": len(table_rows),
        "records_found": len(record_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
        "base_id": cfg["base_id"],
        "table_name": cfg["table_name"],
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_airtable(uid: str) -> dict:
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
