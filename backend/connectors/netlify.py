import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "netlify"
SITES_SOURCE = "netlify_sites"
DEPLOYS_SOURCE = "netlify_deploys"
FORMS_SOURCE = "netlify_forms"
API_BASE = "https://api.netlify.com/api/v1"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[NETLIFY] {message}", flush=True)


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
        raise Exception(f"Netlify API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


def _fetch_user(token: str) -> dict:
    return _request("GET", "/user", token)


def _fetch_sites(token: str) -> list[dict]:
    results = []
    page = 1
    while True:
        data = _request("GET", f"/sites?per_page=100&page={page}", token)
        if not data:
            break
        results.extend(data if isinstance(data, list) else [])
        if len(data) < 100:
            break
        page += 1
    return results


def _fetch_deploys(token: str, site_id: str) -> list[dict]:
    results = []
    page = 1
    fetched = 0
    while fetched < 300:
        data = _request("GET", f"/sites/{site_id}/deploys?per_page=100&page={page}", token)
        if not data or not isinstance(data, list):
            break
        results.extend(data)
        fetched += len(data)
        if len(data) < 100:
            break
        page += 1
    return results


def _fetch_forms(token: str) -> list[dict]:
    data = _request("GET", "/forms", token)
    return data if isinstance(data, list) else []


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


def connect_netlify(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Netlify not configured for this user"}

    try:
        me = _fetch_user(cfg["access_token"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    email = me.get("email") or "Netlify user"

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} email={email}")
    return {
        "status": "success",
        "access_token": _mask_token(cfg.get("access_token")),
        "email": email,
    }


def sync_netlify(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Netlify not configured"}

    token = cfg["access_token"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        sites = _fetch_sites(token)
        forms = _fetch_forms(token)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    site_rows = []
    deploy_rows = []

    for site in sites:
        updated_at = _parse_dt(site.get("updated_at"))
        if last_sync_at and updated_at and updated_at <= last_sync_at:
            pass

        site_rows.append(
            {
                "uid": uid,
                "source": SITES_SOURCE,
                "site_id": site.get("id"),
                "name": site.get("name"),
                "url": site.get("url"),
                "ssl_url": site.get("ssl_url"),
                "custom_domain": site.get("custom_domain"),
                "build_settings": json.dumps(site.get("build_settings") or {}, default=str),
                "created_at": site.get("created_at"),
                "updated_at": site.get("updated_at"),
                "data_json": json.dumps(site, default=str),
                "raw_json": json.dumps(site, default=str),
                "fetched_at": fetched_at,
            }
        )

        site_id = site.get("id")
        if site_id:
            try:
                deploys = _fetch_deploys(token, site_id)
            except Exception as exc:
                _log(f"Failed to fetch deploys for site {site_id}: {exc}")
                deploys = []

            for deploy in deploys:
                created_at = _parse_dt(deploy.get("created_at"))
                if last_sync_at and created_at and created_at <= last_sync_at:
                    continue
                deploy_rows.append(
                    {
                        "uid": uid,
                        "source": DEPLOYS_SOURCE,
                        "deploy_id": deploy.get("id"),
                        "site_id": site_id,
                        "state": deploy.get("state"),
                        "branch": deploy.get("branch"),
                        "commit_ref": deploy.get("commit_ref"),
                        "commit_url": deploy.get("commit_url"),
                        "deploy_url": deploy.get("deploy_url"),
                        "created_at": deploy.get("created_at"),
                        "updated_at": deploy.get("updated_at"),
                        "published_at": deploy.get("published_at"),
                        "data_json": json.dumps(deploy, default=str),
                        "raw_json": json.dumps(deploy, default=str),
                        "fetched_at": fetched_at,
                    }
                )

    form_rows = []
    for form in forms:
        created_at = _parse_dt(form.get("created_at"))
        if last_sync_at and created_at and created_at <= last_sync_at:
            continue
        form_rows.append(
            {
                "uid": uid,
                "source": FORMS_SOURCE,
                "form_id": form.get("id"),
                "site_id": form.get("site_id"),
                "name": form.get("name"),
                "paths": json.dumps(form.get("paths") or [], default=str),
                "submission_count": form.get("submission_count"),
                "created_at": form.get("created_at"),
                "data_json": json.dumps(form, default=str),
                "raw_json": json.dumps(form, default=str),
                "fetched_at": fetched_at,
            }
        )

    total_rows_found += len(site_rows) + len(deploy_rows) + len(form_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, SITES_SOURCE, site_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, DEPLOYS_SOURCE, deploy_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, FORMS_SOURCE, form_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "sites_found": len(site_rows),
        "deploys_found": len(deploy_rows),
        "forms_found": len(form_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_netlify(uid: str) -> dict:
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
