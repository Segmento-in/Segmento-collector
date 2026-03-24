import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "bitbucket"
REPOSITORIES_SOURCE = "bitbucket_repositories"
COMMITS_SOURCE = "bitbucket_commits"
PULLREQUESTS_SOURCE = "bitbucket_pullrequests"
API_BASE = "https://api.bitbucket.org/2.0"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[BITBUCKET] {message}", flush=True)


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


def save_config(uid: str, username: str, app_password: str):
    config = {
        "username": username.strip(),
        "app_password": app_password.strip(),
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
    _log(f"Config saved for uid={uid}")


def _get_headers():
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, username: str, app_password: str, retries: int = 4, **kwargs):
    url = f"{API_BASE}{path}"
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_get_headers())

    for attempt in range(retries):
        response = requests.request(
            method, url,
            headers=headers,
            auth=(username, app_password),
            timeout=40,
            **kwargs
        )

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
        raise Exception(f"Bitbucket API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


def _fetch_user(username: str, app_password: str) -> dict:
    return _request("GET", "/user", username, app_password)


def _fetch_repositories(username: str, app_password: str) -> list[dict]:
    results = []
    url = f"/repositories/{username}?pagelen=100"
    while url:
        data = _request("GET", url, username, app_password)
        results.extend(data.get("values", []))
        next_url = data.get("next")
        if next_url:
            url = next_url.replace(API_BASE, "")
        else:
            url = None
    return results


def _fetch_commits(username: str, app_password: str, repo_slug: str) -> list[dict]:
    results = []
    url = f"/repositories/{username}/{repo_slug}/commits?pagelen=100"
    page = 0
    while url and page < 5:
        data = _request("GET", url, username, app_password)
        results.extend(data.get("values", []))
        next_url = data.get("next")
        if next_url:
            url = next_url.replace(API_BASE, "")
        else:
            url = None
        page += 1
    return results


def _fetch_pullrequests(username: str, app_password: str, repo_slug: str) -> list[dict]:
    results = []
    url = f"/repositories/{username}/{repo_slug}/pullrequests?state=ALL&pagelen=100"
    page = 0
    while url and page < 3:
        data = _request("GET", url, username, app_password)
        results.extend(data.get("values", []))
        next_url = data.get("next")
        if next_url:
            url = next_url.replace(API_BASE, "")
        else:
            url = None
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


def connect_bitbucket(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Bitbucket not configured for this user"}

    try:
        me = _fetch_user(cfg["username"], cfg["app_password"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    display_name = me.get("display_name") or cfg["username"]

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} username={cfg['username']} display_name={display_name}")
    return {
        "status": "success",
        "username": cfg["username"],
        "display_name": display_name,
        "app_password": _mask_token(cfg.get("app_password")),
    }


def sync_bitbucket(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Bitbucket not configured"}

    username = cfg["username"]
    app_password = cfg["app_password"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        repositories = _fetch_repositories(username, app_password)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    repo_rows = []
    commit_rows = []
    pr_rows = []

    for repo in repositories:
        updated_on = _parse_dt(repo.get("updated_on"))
        if last_sync_at and updated_on and updated_on <= last_sync_at:
            pass

        repo_slug = repo.get("slug") or repo.get("full_name", "").split("/")[-1]
        repo_rows.append(
            {
                "uid": uid,
                "source": REPOSITORIES_SOURCE,
                "repo_id": repo.get("uuid"),
                "slug": repo_slug,
                "name": repo.get("name"),
                "full_name": repo.get("full_name"),
                "description": repo.get("description"),
                "is_private": bool(repo.get("is_private")),
                "scm": repo.get("scm"),
                "language": repo.get("language"),
                "size": repo.get("size"),
                "created_on": repo.get("created_on"),
                "updated_on": repo.get("updated_on"),
                "data_json": json.dumps(repo, default=str),
                "raw_json": json.dumps(repo, default=str),
                "fetched_at": fetched_at,
            }
        )

        if repo_slug:
            try:
                commits = _fetch_commits(username, app_password, repo_slug)
            except Exception as exc:
                _log(f"Failed to fetch commits for repo {repo_slug}: {exc}")
                commits = []

            for commit in commits:
                commit_dt = _parse_dt(commit.get("date"))
                if last_sync_at and commit_dt and commit_dt <= last_sync_at:
                    continue
                commit_rows.append(
                    {
                        "uid": uid,
                        "source": COMMITS_SOURCE,
                        "commit_hash": commit.get("hash"),
                        "repo_slug": repo_slug,
                        "repo_full_name": repo.get("full_name"),
                        "message": commit.get("message"),
                        "author_name": ((commit.get("author") or {}).get("raw")),
                        "date": commit.get("date"),
                        "parents": json.dumps([p.get("hash") for p in (commit.get("parents") or [])]),
                        "data_json": json.dumps(commit, default=str),
                        "raw_json": json.dumps(commit, default=str),
                        "fetched_at": fetched_at,
                    }
                )

            try:
                pullrequests = _fetch_pullrequests(username, app_password, repo_slug)
            except Exception as exc:
                _log(f"Failed to fetch pullrequests for repo {repo_slug}: {exc}")
                pullrequests = []

            for pr in pullrequests:
                pr_dt = _parse_dt(pr.get("updated_on"))
                if last_sync_at and pr_dt and pr_dt <= last_sync_at:
                    continue
                pr_rows.append(
                    {
                        "uid": uid,
                        "source": PULLREQUESTS_SOURCE,
                        "pr_id": pr.get("id"),
                        "repo_slug": repo_slug,
                        "repo_full_name": repo.get("full_name"),
                        "title": pr.get("title"),
                        "description": pr.get("description"),
                        "state": pr.get("state"),
                        "author": json.dumps((pr.get("author") or {}), default=str),
                        "source_branch": ((pr.get("source") or {}).get("branch", {}) or {}).get("name"),
                        "destination_branch": ((pr.get("destination") or {}).get("branch", {}) or {}).get("name"),
                        "created_on": pr.get("created_on"),
                        "updated_on": pr.get("updated_on"),
                        "data_json": json.dumps(pr, default=str),
                        "raw_json": json.dumps(pr, default=str),
                        "fetched_at": fetched_at,
                    }
                )

    total_rows_found += len(repo_rows) + len(commit_rows) + len(pr_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, REPOSITORIES_SOURCE, repo_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, COMMITS_SOURCE, commit_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PULLREQUESTS_SOURCE, pr_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "repositories_found": len(repo_rows),
        "commits_found": len(commit_rows),
        "pullrequests_found": len(pr_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_bitbucket(uid: str) -> dict:
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
