import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "cloudflare"
ZONES_SOURCE = "cloudflare_zones"
DNS_SOURCE = "cloudflare_dns_records"
ANALYTICS_SOURCE = "cloudflare_analytics"
API_BASE = "https://api.cloudflare.com/client/v4"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[CLOUDFLARE] {message}", flush=True)


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
        "api_token": (payload.get("api_token") or "").strip(),
        "account_id": (payload.get("account_id") or "").strip(),
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
        raise Exception(f"Cloudflare API error {response.status_code}: {detail}")
    data = response.json()
    if isinstance(data, dict) and not data.get("success", True):
        raise Exception(f"Cloudflare API error: {data.get('errors')}")
    return data.get("result", data) if isinstance(data, dict) else data


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


def connect_cloudflare(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Cloudflare not configured for this user"}

    token = cfg.get("api_token", "")
    account_id = cfg.get("account_id", "")

    try:
        _request("GET", "/zones?per_page=1", token)
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} account={account_id}")
    return {
        "status": "success",
        "api_token": _mask_token(token),
        "account_id": account_id,
    }


def sync_cloudflare(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Cloudflare not configured"}

    token = cfg.get("api_token", "")
    fetched_at = _iso_now()

    try:
        raw_zones = _request("GET", "/zones?per_page=50", token)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    if not isinstance(raw_zones, list):
        raw_zones = []

    dest_cfg = _get_active_destination(uid)

    zone_rows = [
        {
            "uid": uid,
            "source": ZONES_SOURCE,
            "zone_id": z.get("id"),
            "name": z.get("name"),
            "status": z.get("status"),
            "type": z.get("type"),
            "account_id": (z.get("account") or {}).get("id"),
            "name_servers": json.dumps(z.get("name_servers", []), default=str),
            "created_on": z.get("created_on"),
            "modified_on": z.get("modified_on"),
            "raw_json": json.dumps(z, default=str),
            "fetched_at": fetched_at,
        }
        for z in raw_zones
    ]

    dns_rows = []
    analytics_rows = []

    for zone in raw_zones[:20]:
        zone_id = zone.get("id")
        if not zone_id:
            continue
        try:
            raw_dns = _request("GET", f"/zones/{zone_id}/dns_records?per_page=100", token)
            if isinstance(raw_dns, list):
                for d in raw_dns:
                    dns_rows.append({
                        "uid": uid,
                        "source": DNS_SOURCE,
                        "record_id": d.get("id"),
                        "zone_id": zone_id,
                        "zone_name": zone.get("name"),
                        "type": d.get("type"),
                        "name": d.get("name"),
                        "content": d.get("content"),
                        "ttl": d.get("ttl"),
                        "proxied": d.get("proxied"),
                        "created_on": d.get("created_on"),
                        "modified_on": d.get("modified_on"),
                        "raw_json": json.dumps(d, default=str),
                        "fetched_at": fetched_at,
                    })
        except Exception as exc:
            _log(f"DNS fetch failed for zone {zone_id}: {exc}")

    total_pushed = 0
    total_pushed += _push_rows(dest_cfg, SOURCE, ZONES_SOURCE, zone_rows)
    total_pushed += _push_rows(dest_cfg, SOURCE, DNS_SOURCE, dns_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")

    result = {
        "status": "success",
        "zones_found": len(zone_rows),
        "dns_records_found": len(dns_rows),
        "rows_pushed": total_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_cloudflare(uid: str) -> dict:
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}
