import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "jira"
ISSUES_SOURCE = "jira_issues"
PROJECTS_SOURCE = "jira_projects"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[JIRA] {message}", flush=True)


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


def save_config(uid: str, email: str, api_token: str, domain: str):
    config = {
        "email": email.strip(),
        "api_token": api_token.strip(),
        "domain": domain.strip(),
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
    _log(f"Config saved for uid={uid} domain={config['domain']}")


def _get_headers(email: str, api_token: str):
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _request(method: str, url: str, email: str, api_token: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_get_headers(email, api_token))
    auth = (email, api_token)

    for attempt in range(retries):
        response = requests.request(method, url, auth=auth, headers=headers, timeout=40, **kwargs)

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
        raise Exception(f"Jira API error {response.status_code}: {detail}")

    return response.json() if response.text else {}


def _fetch_projects(domain: str, email: str, api_token: str) -> list[dict]:
    """Fetch all Jira projects"""
    url = f"https://{domain}.atlassian.net/rest/api/3/project/search"
    params = {"maxResults": 100}
    
    all_projects = []
    start_at = 0
    
    while True:
        params["startAt"] = start_at
        data = _request("GET", url, email, api_token, params=params)
        
        projects = data.get("values", [])
        all_projects.extend(projects)
        
        if data.get("isLast", True):
            break
            
        start_at += len(projects)
    
    return all_projects


def _fetch_issues(domain: str, email: str, api_token: str, last_sync_at: datetime.datetime | None = None) -> list[dict]:
    """Fetch Jira issues with optional incremental sync"""
    url = f"https://{domain}.atlassian.net/rest/api/3/search"
    
    # Build JQL query
    jql = "ORDER BY updated DESC"
    if last_sync_at:
        timestamp_str = last_sync_at.strftime("%Y-%m-%d %H:%M")
        jql = f"updated >= '{timestamp_str}' ORDER BY updated DESC"
    
    all_issues = []
    start_at = 0
    max_results = 100
    
    while True:
        params = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": "*all",
        }
        
        data = _request("GET", url, email, api_token, params=params)
        
        issues = data.get("issues", [])
        all_issues.extend(issues)
        
        total = data.get("total", 0)
        if start_at + len(issues) >= total:
            break
            
        start_at += max_results
    
    return all_issues


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


def connect_jira(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Jira not configured for this user"}

    try:
        # Test connection by fetching user info
        url = f"https://{cfg['domain']}.atlassian.net/rest/api/3/myself"
        user = _request("GET", url, cfg["email"], cfg["api_token"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} domain={cfg['domain']} user={user.get('displayName')}")
    return {
        "status": "success",
        "email": cfg["email"],
        "display_name": user.get("displayName"),
        "domain": cfg["domain"],
    }


def sync_jira(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Jira not configured"}

    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        projects = _fetch_projects(cfg["domain"], cfg["email"], cfg["api_token"])
        issues = _fetch_issues(cfg["domain"], cfg["email"], cfg["api_token"], last_sync_at)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Projects
    project_rows = []
    for project in projects:
        project_rows.append({
            "uid": uid,
            "source": PROJECTS_SOURCE,
            "project_id": project.get("id"),
            "project_key": project.get("key"),
            "name": project.get("name"),
            "project_type": project.get("projectTypeKey"),
            "lead": (project.get("lead") or {}).get("displayName"),
            "description": project.get("description"),
            "url": project.get("self"),
            "data_json": json.dumps(project, default=str),
            "raw_json": json.dumps(project, default=str),
            "fetched_at": fetched_at,
        })

    # Process Issues
    issue_rows = []
    for issue in issues:
        fields = issue.get("fields", {})
        issue_rows.append({
            "uid": uid,
            "source": ISSUES_SOURCE,
            "issue_id": issue.get("id"),
            "issue_key": issue.get("key"),
            "summary": fields.get("summary"),
            "description": fields.get("description"),
            "issue_type": (fields.get("issuetype") or {}).get("name"),
            "status": (fields.get("status") or {}).get("name"),
            "priority": (fields.get("priority") or {}).get("name"),
            "assignee": (fields.get("assignee") or {}).get("displayName"),
            "reporter": (fields.get("reporter") or {}).get("displayName"),
            "project_key": fields.get("project", {}).get("key"),
            "created": fields.get("created"),
            "updated": fields.get("updated"),
            "resolved": fields.get("resolutiondate"),
            "data_json": json.dumps(issue, default=str),
            "raw_json": json.dumps(issue, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(project_rows) + len(issue_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PROJECTS_SOURCE, project_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, ISSUES_SOURCE, issue_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "projects_found": len(project_rows),
        "issues_found": len(issue_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_jira(uid: str) -> dict:
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
