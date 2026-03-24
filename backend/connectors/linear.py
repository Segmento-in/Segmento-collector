import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "linear"
ISSUES_SOURCE = "linear_issues"
TEAMS_SOURCE = "linear_teams"
PROJECTS_SOURCE = "linear_projects"
API_BASE = "https://api.linear.app/graphql"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[LINEAR] {message}", flush=True)


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
        "Authorization": api_key,
        "Content-Type": "application/json",
    }


def _graphql(query: str, api_key: str, variables: dict | None = None, retries: int = 4) -> dict:
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    headers = _get_headers(api_key)

    for attempt in range(retries):
        response = requests.post(API_BASE, headers=headers, json=payload, timeout=40)

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            try:
                wait_s = int(retry_after) if retry_after else 2 ** attempt
            except Exception:
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
            _log(f"Server error {response.status_code}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        break

    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Linear API error {response.status_code}: {detail}")

    result = response.json()
    if result.get("errors"):
        raise Exception(f"Linear GraphQL error: {result['errors']}")
    return result.get("data") or {}


def _fetch_viewer(api_key: str) -> dict:
    query = """
    query {
      viewer {
        id
        name
        email
      }
    }
    """
    return _graphql(query, api_key).get("viewer") or {}


def _fetch_issues(api_key: str, after: str | None = None) -> list[dict]:
    results = []
    cursor = after
    page = 0
    while page < 10:
        query = """
        query($after: String) {
          issues(first: 100, after: $after, orderBy: updatedAt) {
            nodes {
              id
              identifier
              title
              description
              state { id name type }
              priority
              priorityLabel
              assignee { id name email }
              team { id name key }
              project { id name }
              createdAt
              updatedAt
              completedAt
              canceledAt
              dueDate
              url
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """
        variables = {"after": cursor}
        data = _graphql(query, api_key, variables)
        issues_data = (data.get("issues") or {})
        nodes = issues_data.get("nodes") or []
        results.extend(nodes)
        page_info = issues_data.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break
        page += 1
    return results


def _fetch_teams(api_key: str) -> list[dict]:
    query = """
    query {
      teams(first: 100) {
        nodes {
          id
          name
          key
          description
          timezone
          issueCount
          memberCount: members { nodes { id } }
          createdAt
          updatedAt
        }
      }
    }
    """
    data = _graphql(query, api_key)
    return (data.get("teams") or {}).get("nodes") or []


def _fetch_projects(api_key: str) -> list[dict]:
    results = []
    cursor = None
    page = 0
    while page < 5:
        query = """
        query($after: String) {
          projects(first: 100, after: $after) {
            nodes {
              id
              name
              description
              state
              progress
              startDate
              targetDate
              createdAt
              updatedAt
              url
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        """
        variables = {"after": cursor}
        data = _graphql(query, api_key, variables)
        projects_data = (data.get("projects") or {})
        nodes = projects_data.get("nodes") or []
        results.extend(nodes)
        page_info = projects_data.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break
        page += 1
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


def connect_linear(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Linear not configured for this user"}

    try:
        viewer = _fetch_viewer(cfg["api_key"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    name = viewer.get("name") or "Linear user"
    email = viewer.get("email") or ""

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} name={name} email={email}")
    return {
        "status": "success",
        "api_key": _mask_token(cfg.get("api_key")),
        "name": name,
        "email": email,
    }


def sync_linear(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Linear not configured"}

    api_key = cfg["api_key"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        issues = _fetch_issues(api_key)
        teams = _fetch_teams(api_key)
        projects = _fetch_projects(api_key)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    issue_rows = []
    for issue in issues:
        updated_at = _parse_dt(issue.get("updatedAt"))
        if last_sync_at and updated_at and updated_at <= last_sync_at:
            continue
        issue_rows.append(
            {
                "uid": uid,
                "source": ISSUES_SOURCE,
                "issue_id": issue.get("id"),
                "identifier": issue.get("identifier"),
                "title": issue.get("title"),
                "description": issue.get("description"),
                "state_name": ((issue.get("state") or {}).get("name")),
                "state_type": ((issue.get("state") or {}).get("type")),
                "priority": issue.get("priority"),
                "priority_label": issue.get("priorityLabel"),
                "assignee_name": ((issue.get("assignee") or {}).get("name")),
                "team_name": ((issue.get("team") or {}).get("name")),
                "project_name": ((issue.get("project") or {}).get("name")),
                "created_at": issue.get("createdAt"),
                "updated_at": issue.get("updatedAt"),
                "completed_at": issue.get("completedAt"),
                "canceled_at": issue.get("canceledAt"),
                "due_date": issue.get("dueDate"),
                "url": issue.get("url"),
                "data_json": json.dumps(issue, default=str),
                "raw_json": json.dumps(issue, default=str),
                "fetched_at": fetched_at,
            }
        )

    team_rows = []
    for team in teams:
        team_rows.append(
            {
                "uid": uid,
                "source": TEAMS_SOURCE,
                "team_id": team.get("id"),
                "name": team.get("name"),
                "key": team.get("key"),
                "description": team.get("description"),
                "timezone": team.get("timezone"),
                "issue_count": team.get("issueCount"),
                "created_at": team.get("createdAt"),
                "updated_at": team.get("updatedAt"),
                "data_json": json.dumps(team, default=str),
                "raw_json": json.dumps(team, default=str),
                "fetched_at": fetched_at,
            }
        )

    project_rows = []
    for project in projects:
        updated_at = _parse_dt(project.get("updatedAt"))
        if last_sync_at and updated_at and updated_at <= last_sync_at:
            continue
        project_rows.append(
            {
                "uid": uid,
                "source": PROJECTS_SOURCE,
                "project_id": project.get("id"),
                "name": project.get("name"),
                "description": project.get("description"),
                "state": project.get("state"),
                "progress": project.get("progress"),
                "start_date": project.get("startDate"),
                "target_date": project.get("targetDate"),
                "created_at": project.get("createdAt"),
                "updated_at": project.get("updatedAt"),
                "url": project.get("url"),
                "data_json": json.dumps(project, default=str),
                "raw_json": json.dumps(project, default=str),
                "fetched_at": fetched_at,
            }
        )

    total_rows_found += len(issue_rows) + len(team_rows) + len(project_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, ISSUES_SOURCE, issue_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, TEAMS_SOURCE, team_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PROJECTS_SOURCE, project_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "issues_found": len(issue_rows),
        "teams_found": len(team_rows),
        "projects_found": len(project_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_linear(uid: str) -> dict:
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
