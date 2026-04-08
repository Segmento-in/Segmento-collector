import datetime
import json
import sqlite3
import os
from urllib.parse import urlencode

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = os.getenv("DB_PATH", "/tmp/identity.db")
SOURCE = "power_bi"
WORKSPACES_SOURCE = "power_bi_workspaces"
REPORTS_SOURCE = "power_bi_reports"
DATASETS_SOURCE = "power_bi_datasets"
API_BASE = "https://api.powerbi.com/v1.0/myorg"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _iso_now():
    return datetime.datetime.now(datetime.UTC).isoformat()


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


def _mask_secret(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}{'*' * max(len(value) - 8, 4)}{value[-4:]}"


def _get_config(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "SELECT config_json FROM connector_configs WHERE uid=? AND connector=? LIMIT 1",
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
        "SELECT state_json FROM connector_state WHERE uid=? AND source=? LIMIT 1",
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
        "UPDATE connector_configs SET status=? WHERE uid=? AND connector=?",
        (status, uid, SOURCE),
    )
    con.commit()
    con.close()


def _set_connection_enabled(uid: str, enabled: bool):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        "UPDATE google_connections SET enabled=? WHERE uid=? AND source=?",
        (1 if enabled else 0, uid, SOURCE),
    )
    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO google_connections (uid, source, enabled) VALUES (?, ?, ?)",
            (uid, SOURCE, 1 if enabled else 0),
        )
    con.commit()
    con.close()


def save_config(uid: str, payload: dict):
    config = {
        "client_id": (payload.get("client_id") or "").strip(),
        "client_secret": (payload.get("client_secret") or "").strip(),
        "tenant_id": (payload.get("tenant_id") or "").strip(),
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


def _get_access_token(cfg: dict) -> str:
    response = requests.post(
        f"https://login.microsoftonline.com/{cfg['tenant_id']}/oauth2/v2.0/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=urlencode({
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "scope": "https://analysis.windows.net/powerbi/api/.default",
            "grant_type": "client_credentials",
        }),
        timeout=10,
    )
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Power BI auth error {response.status_code}: {detail}")
    token = response.json().get("access_token")
    if not token:
        raise Exception("Power BI token response missing access_token")
    return token


def _request(path: str, token: str, params=None) -> dict:
    response = requests.get(
        f"{API_BASE}{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params=params,
        timeout=10,
    )
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"Power BI API error {response.status_code}: {detail}")
    return response.json() if response.text else {}


def _filter_incremental(rows: list[dict], last_sync_at):
    if not last_sync_at:
        return rows
    filtered = []
    for row in rows:
        updated_at = _parse_dt(row.get("createdDate") or row.get("modifiedDateTime"))
        if not updated_at or updated_at > last_sync_at:
            filtered.append(row)
    return filtered


def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg or not rows:
        return 0
    return push_to_destination(dest_cfg, route_source, rows)


def connect_power_bi(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Power BI not configured"}
    try:
        token = _get_access_token(cfg)
        groups = _request("/groups", token, params={"$top": 1}).get("value", [])
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}
    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    return {
        "status": "success",
        "client_id": cfg.get("client_id"),
        "tenant_id": cfg.get("tenant_id"),
        "client_secret": _mask_secret(cfg.get("client_secret")),
        "workspaces_visible": len(groups),
    }


def sync_power_bi(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Power BI not configured"}
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None
    try:
        token = _get_access_token(cfg)
        groups = _request("/groups", token).get("value", [])
        reports = []
        datasets = []
        for group in groups:
            group_id = group.get("id")
            if not group_id:
                continue
            group_reports = _request(f"/groups/{group_id}/reports", token).get("value", [])
            for item in group_reports:
                item["groupId"] = group_id
            reports.extend(group_reports)
            group_datasets = _request(f"/groups/{group_id}/datasets", token).get("value", [])
            for item in group_datasets:
                item["workspaceId"] = group_id
            datasets.extend(group_datasets)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    groups = _filter_incremental(groups, last_sync_at)
    reports = _filter_incremental(reports, last_sync_at)
    datasets = _filter_incremental(datasets, last_sync_at)

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    workspace_rows = [{
        "uid": uid,
        "source": WORKSPACES_SOURCE,
        "workspace_id": item.get("id"),
        "name": item.get("name"),
        "is_read_only": item.get("isReadOnly"),
        "is_on_dedicated_capacity": item.get("isOnDedicatedCapacity"),
        "capacity_id": item.get("capacityId"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in groups]

    report_rows = [{
        "uid": uid,
        "source": REPORTS_SOURCE,
        "report_id": item.get("id"),
        "workspace_id": item.get("groupId"),
        "dataset_id": item.get("datasetId"),
        "name": item.get("name"),
        "web_url": item.get("webUrl"),
        "embed_url": item.get("embedUrl"),
        "report_type": item.get("reportType"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in reports]

    dataset_rows = [{
        "uid": uid,
        "source": DATASETS_SOURCE,
        "dataset_id": item.get("id"),
        "workspace_id": item.get("workspaceId"),
        "name": item.get("name"),
        "configured_by": item.get("configuredBy"),
        "is_refreshable": item.get("isRefreshable"),
        "is_effective_identity_required": item.get("isEffectiveIdentityRequired"),
        "is_effective_identity_roles_required": item.get("isEffectiveIdentityRolesRequired"),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in datasets]

    rows_found = len(workspace_rows) + len(report_rows) + len(dataset_rows)
    rows_pushed = 0
    rows_pushed += _push_rows(dest_cfg, SOURCE, WORKSPACES_SOURCE, workspace_rows)
    rows_pushed += _push_rows(dest_cfg, SOURCE, REPORTS_SOURCE, report_rows)
    rows_pushed += _push_rows(dest_cfg, SOURCE, DATASETS_SOURCE, dataset_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "workspaces_found": len(workspace_rows),
        "reports_found": len(report_rows),
        "datasets_found": len(dataset_rows),
        "rows_found": rows_found,
        "rows_pushed": rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_power_bi(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
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
