import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "typeform"
FORMS_SOURCE = "typeform_forms"
RESPONSES_SOURCE = "typeform_responses"
WORKSPACES_SOURCE = "typeform_workspaces"
API_BASE = "https://api.typeform.com"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[TYPEFORM] {message}", flush=True)


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


def _get_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
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
        raise Exception(f"Typeform API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


def _fetch_forms(token: str) -> list[dict]:
    results = []
    page = 1
    page_size = 200

    while True:
        data = _request("GET", "/forms", token, params={"page": page, "page_size": page_size})
        items = data.get("items") or []
        results.extend(items)

        total_items = data.get("total_items", 0)
        if len(results) >= total_items or not items:
            break

        page += 1
        time.sleep(0.1)

    return results


def _fetch_responses(token: str, form_id: str, since: str | None = None) -> list[dict]:
    results = []
    before = None

    while True:
        params: dict = {"page_size": 200}
        if since:
            params["since"] = since
        if before:
            params["before"] = before

        data = _request("GET", f"/forms/{form_id}/responses", token, params=params)
        items = data.get("items") or []
        results.extend(items)

        total_items = data.get("total_items", 0)
        if len(results) >= total_items or not items:
            break

        before = items[-1].get("token")
        if not before:
            break

        time.sleep(0.1)

    return results


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


def connect_typeform(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Typeform not configured for this user"}

    try:
        me = _request("GET", "/me", cfg["access_token"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    alias = me.get("alias") or me.get("email") or "Typeform user"
    email = me.get("email") or ""

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} alias={alias}")
    return {
        "status": "success",
        "access_token": _mask_token(cfg.get("access_token")),
        "alias": alias,
        "email": email,
    }


def sync_typeform(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Typeform not configured"}

    token = cfg["access_token"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None
    since_str = last_sync_at.strftime("%Y-%m-%dT%H:%M:%S") if last_sync_at else None

    try:
        forms = _fetch_forms(token)
        workspaces_data = _request("GET", "/workspaces", token, params={"page_size": 200})
        workspaces = workspaces_data.get("items") or []
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    workspace_rows = []
    for ws in workspaces:
        workspace_rows.append({
            "uid": uid,
            "source": WORKSPACES_SOURCE,
            "workspace_id": ws.get("id"),
            "name": ws.get("name"),
            "forms_count": (ws.get("forms") or {}).get("count"),
            "members_count": (ws.get("members") or {}).get("count"),
            "shared": bool(ws.get("shared")),
            "default": bool(ws.get("default")),
            "data_json": json.dumps(ws, default=str),
            "raw_json": json.dumps(ws, default=str),
            "fetched_at": fetched_at,
        })

    form_rows = []
    response_rows = []

    for form in forms:
        form_id = form.get("id")
        form_rows.append({
            "uid": uid,
            "source": FORMS_SOURCE,
            "form_id": form_id,
            "title": form.get("title"),
            "theme_id": (form.get("theme") or {}).get("href"),
            "workspace_id": (form.get("workspace") or {}).get("href"),
            "responses_count": (form.get("_links") or {}).get("responses"),
            "last_updated_at": form.get("last_updated_at"),
            "created_at": form.get("created_at"),
            "settings": json.dumps(form.get("settings") or {}, default=str),
            "data_json": json.dumps(form, default=str),
            "raw_json": json.dumps(form, default=str),
            "fetched_at": fetched_at,
        })

        if form_id:
            try:
                responses = _fetch_responses(token, form_id, since=since_str)
            except Exception as exc:
                _log(f"Failed to fetch responses for form {form_id}: {exc}")
                responses = []

            for resp in responses:
                answers = resp.get("answers") or []
                response_rows.append({
                    "uid": uid,
                    "source": RESPONSES_SOURCE,
                    "response_id": resp.get("response_id") or resp.get("token"),
                    "form_id": form_id,
                    "landed_at": resp.get("landed_at"),
                    "submitted_at": resp.get("submitted_at"),
                    "answers_count": len(answers),
                    "answers_json": json.dumps(answers, default=str),
                    "hidden": json.dumps(resp.get("hidden") or {}, default=str),
                    "calculated_score": (resp.get("calculated") or {}).get("score"),
                    "data_json": json.dumps(resp, default=str),
                    "raw_json": json.dumps(resp, default=str),
                    "fetched_at": fetched_at,
                })

    total_rows_found = len(form_rows) + len(response_rows) + len(workspace_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, FORMS_SOURCE, form_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, RESPONSES_SOURCE, response_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, WORKSPACES_SOURCE, workspace_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "forms_found": len(form_rows),
        "responses_found": len(response_rows),
        "workspaces_found": len(workspace_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_typeform(uid: str) -> dict:
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
