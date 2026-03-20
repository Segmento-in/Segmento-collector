import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = "identity.db"
SOURCE = "stripe"
API_BASE = "https://api.stripe.com/v1"
PAGE_SIZE = 100


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message):
    print(f"[STRIPE] {message}", flush=True)


def _request(method, path, secret_key, params=None, retries=4):
    url = f"{API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {secret_key}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    for attempt in range(retries):
        try:
            res = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                timeout=45,
            )
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            wait_s = min(2 ** attempt, 10)
            _log(f"network error: {exc}; retrying in {wait_s}s")
            time.sleep(wait_s)
            continue

        if res.status_code == 429:
            if attempt == retries - 1:
                break
            retry_after = res.headers.get("Retry-After")
            try:
                wait_s = max(int(retry_after), 1) if retry_after else min(2 ** attempt, 10)
            except Exception:
                wait_s = min(2 ** attempt, 10)
            _log(f"rate limited; retrying in {wait_s}s")
            time.sleep(wait_s)
            continue

        if res.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                break
            wait_s = min(2 ** attempt, 10)
            _log(f"server error {res.status_code}; retrying in {wait_s}s")
            time.sleep(wait_s)
            continue

        break

    if res.status_code in (401, 403):
        raise PermissionError(f"Stripe API auth failed ({res.status_code})")
    if res.status_code >= 400:
        try:
            payload = res.json()
            err = payload.get("error") or {}
            msg = err.get("message") or res.text[:300]
        except Exception:
            msg = res.text[:300]
        raise Exception(f"Stripe API error ({res.status_code}): {msg}")

    return res.json()


def _get_config(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
        """,
        (uid, SOURCE),
    )
    row = fetchone_secure(cur)
    con.close()
    return row


def save_credentials(uid, secret_key):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, api_key, status, created_at)
        VALUES (?, ?, ?, 'configured', ?)
        """,
        (
            uid,
            SOURCE,
            encrypt_value(secret_key),
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )
    con.commit()
    con.close()


def _get_secret_key(uid):
    row = _get_config(uid)
    if not row or not row.get("api_key"):
        raise Exception("Stripe secret key not configured")
    return row["api_key"]


def get_state(uid):
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
    if not row:
        return {"last_sync_ts": None}
    try:
        return json.loads(row["state_json"])
    except Exception:
        return {"last_sync_ts": None}


def save_state(uid, state):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            uid,
            SOURCE,
            json.dumps(state),
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )
    con.commit()
    con.close()


def get_active_destination(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        ORDER BY id DESC
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


def _account_display_name(account):
    profile = account.get("business_profile") or {}
    return (
        account.get("business_name")
        or profile.get("name")
        or account.get("email")
        or account.get("id")
    )


def connect_stripe(uid):
    try:
        secret_key = _get_secret_key(uid)
        account = _request("GET", "/account", secret_key)
    except Exception as exc:
        return {"status": "error", "message": str(exc)}

    account_id = account.get("id")
    display_name = _account_display_name(account)

    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO google_connections
        (uid, source, enabled)
        VALUES (?, ?, 1)
        """,
        (uid, SOURCE),
    )
    cur.execute(
        """
        INSERT OR REPLACE INTO stripe_connections
        (uid, account_id, display_name, connected_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            uid,
            account_id,
            display_name,
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )
    cur.execute(
        """
        UPDATE connector_configs
        SET status='connected'
        WHERE uid=? AND connector=?
        """,
        (uid, SOURCE),
    )
    con.commit()
    con.close()

    return {
        "status": "success",
        "account_id": account_id,
        "display_name": display_name,
    }


def _to_epoch(value):
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _normalize_record(uid, resource, item, fetched_at):
    record_id = item.get("id")
    created_ts = _to_epoch(item.get("created"))
    created_at = (
        datetime.datetime.fromtimestamp(created_ts, tz=datetime.UTC).isoformat()
        if created_ts
        else None
    )
    return {
        "uid": uid,
        "source": SOURCE,
        "entity": resource,
        "record_id": record_id,
        "object": item.get("object"),
        "created": created_ts,
        "created_at": created_at,
        "fetched_at": fetched_at,
        "data_json": json.dumps(item, default=str),
        "raw_json": json.dumps(item, default=str),
    }


def _fetch_paginated(secret_key, resource, created_gte=None):
    rows = []
    starting_after = None

    for _ in range(10000):
        params = {"limit": PAGE_SIZE}
        if resource == "subscriptions":
            params["status"] = "all"
        if starting_after:
            params["starting_after"] = starting_after
        if created_gte:
            params["created[gte]"] = created_gte

        payload = _request("GET", f"/{resource}", secret_key, params=params)
        data = payload.get("data") or []
        rows.extend(data)

        if not payload.get("has_more") or not data:
            break

        starting_after = data[-1].get("id")
        if not starting_after:
            break

    return rows


def sync_stripe(uid, sync_type="historical"):
    try:
        secret_key = _get_secret_key(uid)
        state = get_state(uid)
        created_gte = None
        if sync_type == "incremental" and state.get("last_sync_ts"):
            created_gte = int(state["last_sync_ts"]) + 1

        fetched_at = datetime.datetime.now(datetime.UTC).isoformat()
        resources = ("customers", "charges", "subscriptions", "products")
        rows = []
        counts = {}
        max_created = None

        for resource in resources:
            items = _fetch_paginated(secret_key, resource, created_gte=created_gte)
            counts[resource] = len(items)
            for item in items:
                record = _normalize_record(uid, resource, item, fetched_at)
                rows.append(record)
                created_ts = _to_epoch(item.get("created"))
                if created_ts is not None:
                    max_created = created_ts if max_created is None else max(max_created, created_ts)

        next_sync_ts = max_created or int(datetime.datetime.now(datetime.UTC).timestamp())
        save_state(uid, {"last_sync_ts": next_sync_ts})

        dest_cfg = get_active_destination(uid)
        if not dest_cfg:
            return {
                "status": "success",
                "customers": counts.get("customers", 0),
                "charges": counts.get("charges", 0),
                "subscriptions": counts.get("subscriptions", 0),
                "products": counts.get("products", 0),
                "rows_found": len(rows),
                "rows_pushed": 0,
                "sync_type": sync_type,
                "message": "No active destination",
            }

        pushed = push_to_destination(dest_cfg, SOURCE, rows) if rows else 0
        return {
            "status": "success",
            "customers": counts.get("customers", 0),
            "charges": counts.get("charges", 0),
            "subscriptions": counts.get("subscriptions", 0),
            "products": counts.get("products", 0),
            "rows_found": len(rows),
            "rows_pushed": pushed,
            "sync_type": sync_type,
        }

    except PermissionError as exc:
        _log(f"sync auth error: {exc}")
        return {"status": "error", "message": str(exc)}
    except Exception as exc:
        _log(f"sync failed: {exc}")
        return {"status": "error", "message": str(exc)}


def disconnect_stripe(uid):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE google_connections
        SET enabled=0
        WHERE uid=? AND source=?
        """,
        (uid, SOURCE),
    )
    cur.execute("DELETE FROM stripe_connections WHERE uid=?", (uid,))
    cur.execute(
        """
        UPDATE connector_configs
        SET status='disconnected'
        WHERE uid=? AND connector=?
        """,
        (uid, SOURCE),
    )
    con.commit()
    con.close()

    return {"status": "disconnected"}
