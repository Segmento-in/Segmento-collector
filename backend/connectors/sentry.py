import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "sentry"
PROJECTS_SOURCE = "sentry_projects"
ISSUES_SOURCE = "sentry_issues"
EVENTS_SOURCE = "sentry_events"
API_BASE = "https://sentry.io/api/0"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[SENTRY] {message}", flush=True)


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * max(len(token) - 8, 4)}{token[-4:]}"


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


def save_config(uid: str, payload: dict):
    config = {
        "access_token": (payload.get("access_token") or "").strip(),
        "organization_slug": (payload.get("organization_slug") or "").strip(),
    }

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, status, created_at)
        VALUES (?, ?, ?, 'pending', ?)
        """,
        (uid, SOURCE, encrypt_value(json.dumps(config)), _iso_now()),
    )
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}")


def _get_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, token: str, retries: int = 3, **kwargs):
    url = f"{API_BASE}{path}"
    headers = _get_headers(token)
    for attempt in range(retries):
        response = requests.request(method, url, headers=headers, timeout=30, **kwargs)
        if response.status_code == 429:
            wait_s = 2 ** attempt
            if attempt == retries - 1:
                break
            _log(f"Rate limited; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue
        if response.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                break
            wait_s = min(2 ** attempt, 15)
            time.sleep(wait_s)
            continue
        break
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Sentry API error {response.status_code}: {detail}")
    return response.json()


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


def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg:
        _log(f"No active destination configured. Skipping push for {label}")
        return 0
    if not rows:
        _log(f"No rows generated for {label}. Skipping push")
        return 0
    _log(f"Pushing {len(rows)} rows → {label}")
    pushed = push_to_destination(dest_cfg, route_source, rows)
    _log(f"Push complete → {label}: {pushed} rows")
    return pushed


def connect_sentry(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Sentry not configured for this user"}

    token = cfg.get("access_token", "")
    org_slug = cfg.get("organization_slug", "")

    try:
        _request("GET", f"/organizations/{org_slug}/projects/", token)
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} org={org_slug}")
    return {
        "status": "success",
        "access_token": _mask_token(token),
        "organization_slug": org_slug,
    }


def sync_sentry(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Sentry not configured"}

    token = cfg.get("access_token", "")
    org_slug = cfg.get("organization_slug", "")
    fetched_at = _iso_now()

    try:
        raw_projects = _request("GET", f"/organizations/{org_slug}/projects/", token)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    if not isinstance(raw_projects, list):
        raw_projects = []

    dest_cfg = _get_active_destination(uid)

    project_rows = [
        {
            "uid": uid,
            "source": PROJECTS_SOURCE,
            "project_id": p.get("id"),
            "slug": p.get("slug"),
            "name": p.get("name"),
            "platform": p.get("platform"),
            "status": p.get("status"),
            "date_created": p.get("dateCreated"),
            "first_event": p.get("firstEvent"),
            "raw_json": json.dumps(p, default=str),
            "fetched_at": fetched_at,
        }
        for p in raw_projects
    ]

    issue_rows = []
    event_rows = []

    for project in raw_projects[:10]:
        proj_slug = project.get("slug")
        if not proj_slug:
            continue
        try:
            raw_issues = _request("GET", f"/projects/{org_slug}/{proj_slug}/issues/", token)
            if isinstance(raw_issues, list):
                for iss in raw_issues[:100]:
                    issue_rows.append({
                        "uid": uid,
                        "source": ISSUES_SOURCE,
                        "issue_id": iss.get("id"),
                        "project_slug": proj_slug,
                        "title": iss.get("title"),
                        "culprit": iss.get("culprit"),
                        "level": iss.get("level"),
                        "status": iss.get("status"),
                        "times_seen": iss.get("count"),
                        "first_seen": iss.get("firstSeen"),
                        "last_seen": iss.get("lastSeen"),
                        "raw_json": json.dumps(iss, default=str),
                        "fetched_at": fetched_at,
                    })
        except Exception as exc:
            _log(f"Issues fetch failed for project {proj_slug}: {exc}")

    total_pushed = 0
    total_pushed += _push_rows(dest_cfg, SOURCE, PROJECTS_SOURCE, project_rows)
    total_pushed += _push_rows(dest_cfg, SOURCE, ISSUES_SOURCE, issue_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")

    result = {
        "status": "success",
        "projects_found": len(project_rows),
        "issues_found": len(issue_rows),
        "rows_pushed": total_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_sentry(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}
