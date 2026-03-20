import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = "identity.db"
SOURCE = "hubspot"
CONTACTS_SOURCE = "hubspot_contacts"
COMPANIES_SOURCE = "hubspot_companies"
DEALS_SOURCE = "hubspot_deals"
API_BASE = "https://api.hubapi.com"
PAGE_SIZE = 100

CONTACT_PROPERTIES = [
    "firstname",
    "lastname",
    "email",
    "phone",
    "company",
    "jobtitle",
    "createdate",
    "lastmodifieddate",
    "hs_object_id",
]
COMPANY_PROPERTIES = [
    "name",
    "domain",
    "industry",
    "phone",
    "city",
    "country",
    "createdate",
    "hs_lastmodifieddate",
    "hs_object_id",
]
DEAL_PROPERTIES = [
    "dealname",
    "amount",
    "dealstage",
    "pipeline",
    "closedate",
    "createdate",
    "hs_lastmodifieddate",
    "hs_object_id",
]


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[HUBSPOT] {message}", flush=True)


def _iso_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


def _parse_dt(value):
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        dt = datetime.datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.UTC)
        return dt.astimezone(datetime.UTC)
    except Exception:
        return None


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}{'*' * max(len(token) - 8, 4)}{token[-4:]}"


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


def _headers(token: str):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, token: str, params=None, retries: int = 5):
    url = f"{API_BASE}{path}"

    for attempt in range(retries):
        try:
            res = requests.request(
                method,
                url,
                headers=_headers(token),
                params=params,
                timeout=45,
            )
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            wait_s = min(2 ** attempt, 20)
            _log(f"network error on {path}: {exc}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if res.status_code == 429:
            if attempt == retries - 1:
                break
            retry_after = res.headers.get("Retry-After")
            try:
                wait_s = max(int(retry_after), 1) if retry_after else min(2 ** attempt, 20)
            except Exception:
                wait_s = min(2 ** attempt, 20)
            _log(f"rate limited on {path}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        if res.status_code in (500, 502, 503, 504):
            if attempt == retries - 1:
                break
            wait_s = min(2 ** attempt, 20)
            _log(f"server error {res.status_code} on {path}; sleeping {wait_s}s")
            time.sleep(wait_s)
            continue

        break

    if res.status_code in (401, 403):
        raise PermissionError(f"HubSpot API auth failed ({res.status_code})")
    if res.status_code >= 400:
        try:
            detail = res.json()
        except Exception:
            detail = res.text[:300]
        raise Exception(f"HubSpot API error ({res.status_code}): {detail}")

    return res.json()


def _object_updated_at(item: dict) -> str | None:
    props = item.get("properties") or {}
    return (
        item.get("updatedAt")
        or props.get("lastmodifieddate")
        or props.get("hs_lastmodifieddate")
        or props.get("updatedAt")
    )


def _object_created_at(item: dict) -> str | None:
    props = item.get("properties") or {}
    return item.get("createdAt") or props.get("createdate")


def _fetch_objects(token: str, object_type: str, properties: list[str]) -> list[dict]:
    results = []
    after = None

    for _ in range(10000):
        params = {
            "limit": PAGE_SIZE,
            "archived": "false",
            "properties": ",".join(properties),
        }
        if after:
            params["after"] = after

        payload = _request("GET", f"/crm/v3/objects/{object_type}", token, params=params)
        batch = payload.get("results") or []
        results.extend(batch)

        paging = payload.get("paging") or {}
        next_after = ((paging.get("next") or {}).get("after"))
        if not next_after:
            break
        after = next_after

    return results


def _filter_incremental(items: list[dict], last_sync_at):
    if not last_sync_at:
        return items
    filtered = []
    for item in items:
        updated_at = _parse_dt(_object_updated_at(item))
        if updated_at and updated_at > last_sync_at:
            filtered.append(item)
    return filtered


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


def connect_hubspot(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "HubSpot not configured for this user"}

    try:
        payload = _request(
            "GET",
            "/crm/v3/objects/contacts",
            cfg["access_token"],
            params={"limit": 1, "archived": "false"},
        )
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    visible_contacts = len(payload.get("results") or [])
    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid}")
    return {
        "status": "success",
        "access_token": _mask_token(cfg.get("access_token")),
        "contacts_visible": visible_contacts,
    }


def sync_hubspot(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "HubSpot not configured"}

    token = cfg["access_token"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        contacts = _fetch_objects(token, "contacts", CONTACT_PROPERTIES)
        companies = _fetch_objects(token, "companies", COMPANY_PROPERTIES)
        deals = _fetch_objects(token, "deals", DEAL_PROPERTIES)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    contacts = _filter_incremental(contacts, last_sync_at)
    companies = _filter_incremental(companies, last_sync_at)
    deals = _filter_incremental(deals, last_sync_at)

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    contact_rows = []
    for item in contacts:
        props = item.get("properties") or {}
        contact_rows.append(
            {
                "uid": uid,
                "source": CONTACTS_SOURCE,
                "record_id": item.get("id"),
                "object_type": "contact",
                "firstname": props.get("firstname"),
                "lastname": props.get("lastname"),
                "email": props.get("email"),
                "phone": props.get("phone"),
                "company": props.get("company"),
                "jobtitle": props.get("jobtitle"),
                "created_at": _object_created_at(item),
                "updated_at": _object_updated_at(item),
                "archived": bool(item.get("archived")),
                "data_json": json.dumps(props, default=str),
                "raw_json": json.dumps(item, default=str),
                "fetched_at": fetched_at,
            }
        )

    company_rows = []
    for item in companies:
        props = item.get("properties") or {}
        company_rows.append(
            {
                "uid": uid,
                "source": COMPANIES_SOURCE,
                "record_id": item.get("id"),
                "object_type": "company",
                "name": props.get("name"),
                "domain": props.get("domain"),
                "industry": props.get("industry"),
                "phone": props.get("phone"),
                "city": props.get("city"),
                "country": props.get("country"),
                "created_at": _object_created_at(item),
                "updated_at": _object_updated_at(item),
                "archived": bool(item.get("archived")),
                "data_json": json.dumps(props, default=str),
                "raw_json": json.dumps(item, default=str),
                "fetched_at": fetched_at,
            }
        )

    deal_rows = []
    for item in deals:
        props = item.get("properties") or {}
        deal_rows.append(
            {
                "uid": uid,
                "source": DEALS_SOURCE,
                "record_id": item.get("id"),
                "object_type": "deal",
                "dealname": props.get("dealname"),
                "amount": props.get("amount"),
                "dealstage": props.get("dealstage"),
                "pipeline": props.get("pipeline"),
                "closedate": props.get("closedate"),
                "created_at": _object_created_at(item),
                "updated_at": _object_updated_at(item),
                "archived": bool(item.get("archived")),
                "data_json": json.dumps(props, default=str),
                "raw_json": json.dumps(item, default=str),
                "fetched_at": fetched_at,
            }
        )

    total_rows_found = len(contact_rows) + len(company_rows) + len(deal_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, CONTACTS_SOURCE, contact_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, COMPANIES_SOURCE, company_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, DEALS_SOURCE, deal_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "contacts_found": len(contact_rows),
        "companies_found": len(company_rows),
        "deals_found": len(deal_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_hubspot(uid: str) -> dict:
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
