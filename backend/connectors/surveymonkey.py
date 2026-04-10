import datetime
import json
import sqlite3
import os
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = os.getenv("DB_PATH", "identity.db")
SOURCE = "surveymonkey"
SURVEYS_SOURCE = "surveymonkey_surveys"
RESPONSES_SOURCE = "surveymonkey_responses"
COLLECTORS_SOURCE = "surveymonkey_collectors"
API_BASE = "https://api.surveymonkey.com/v3"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[SURVEYMONKEY] {message}", flush=True)


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
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def _iso_now():
    return datetime.datetime.utcnow().isoformat()


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
    try:
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
    except Exception as e:
        pass


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
        raise Exception(f"SurveyMonkey API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


def _fetch_paginated(token: str, path: str, params: dict | None = None) -> list[dict]:
    results = []
    page = 1
    per_page = 100
    base_params = dict(params or {})
    base_params["per_page"] = per_page

    while True:
        base_params["page"] = page
        data = _request("GET", path, token, params=base_params)
        items = data.get("data") or []
        results.extend(items)

        links = data.get("links") or {}
        if not links.get("next") or not items:
            break

        page += 1
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


def connect_surveymonkey(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "SurveyMonkey not configured for this user"}

    try:
        me = _request("GET", "/users/me", cfg["access_token"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    username = me.get("username") or me.get("first_name") or "SurveyMonkey user"
    email = me.get("email") or ""

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} username={username}")
    return {
        "status": "success",
        "access_token": _mask_token(cfg.get("access_token")),
        "username": username,
        "email": email,
    }


def sync_surveymonkey(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "SurveyMonkey not configured"}

    token = cfg["access_token"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    extra_params: dict = {}
    if last_sync_at:
        extra_params["start_modified_at"] = last_sync_at.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        surveys = _fetch_paginated(token, "/surveys", params=extra_params)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    survey_rows = []
    response_rows = []
    collector_rows = []

    for survey in surveys:
        survey_id = survey.get("id")
        survey_rows.append({
            "uid": uid,
            "source": SURVEYS_SOURCE,
            "survey_id": survey_id,
            "title": survey.get("title"),
            "nickname": survey.get("nickname"),
            "language": survey.get("language"),
            "question_count": survey.get("question_count"),
            "page_count": survey.get("page_count"),
            "response_count": survey.get("response_count"),
            "category": survey.get("category"),
            "date_created": survey.get("date_created"),
            "date_modified": survey.get("date_modified"),
            "data_json": json.dumps(survey, default=str),
            "raw_json": json.dumps(survey, default=str),
            "fetched_at": fetched_at,
        })

        if survey_id:
            # Responses
            try:
                responses = _fetch_paginated(token, f"/surveys/{survey_id}/responses/bulk")
                for resp in responses:
                    pages = resp.get("pages") or []
                    answer_count = sum(len(p.get("questions") or []) for p in pages)
                    response_rows.append({
                        "uid": uid,
                        "source": RESPONSES_SOURCE,
                        "response_id": resp.get("id"),
                        "survey_id": survey_id,
                        "collector_id": resp.get("collector_id"),
                        "respondent_id": resp.get("respondent_id"),
                        "total_time": resp.get("total_time"),
                        "ip_address": resp.get("ip_address"),
                        "date_created": resp.get("date_created"),
                        "date_modified": resp.get("date_modified"),
                        "answer_count": answer_count,
                        "pages_json": json.dumps(pages, default=str),
                        "data_json": json.dumps(resp, default=str),
                        "raw_json": json.dumps(resp, default=str),
                        "fetched_at": fetched_at,
                    })
            except Exception as exc:
                _log(f"Failed to fetch responses for survey {survey_id}: {exc}")

            # Collectors
            try:
                collectors = _fetch_paginated(token, f"/surveys/{survey_id}/collectors")
                for col in collectors:
                    collector_rows.append({
                        "uid": uid,
                        "source": COLLECTORS_SOURCE,
                        "collector_id": col.get("id"),
                        "survey_id": survey_id,
                        "name": col.get("name"),
                        "type": col.get("type"),
                        "status": col.get("status"),
                        "response_count": col.get("response_count"),
                        "redirect_url": col.get("redirect_url"),
                        "date_created": col.get("date_created"),
                        "date_modified": col.get("date_modified"),
                        "data_json": json.dumps(col, default=str),
                        "raw_json": json.dumps(col, default=str),
                        "fetched_at": fetched_at,
                    })
            except Exception as exc:
                _log(f"Failed to fetch collectors for survey {survey_id}: {exc}")

    total_rows_found = len(survey_rows) + len(response_rows) + len(collector_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, SURVEYS_SOURCE, survey_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, RESPONSES_SOURCE, response_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, COLLECTORS_SOURCE, collector_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "surveys_found": len(survey_rows),
        "responses_found": len(response_rows),
        "collectors_found": len(collector_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_surveymonkey(uid: str) -> dict:

    try:
        _set_connection_enabled(uid, False)
        _update_status(uid, "disconnected")
        _log(f"Disconnected uid={uid}")
        return {"status": "success"}
    
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}

def _get_active_destination(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT dest_type, host, port, username, password, database_name, format
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
        "type": row.get("dest_type"),
        "host": row.get("host"),
        "port": row.get("port"),
        "username": row.get("username"),
        "password": row.get("password"),
        "database_name": row.get("database_name"),
        "format": row.get("format")
    }

