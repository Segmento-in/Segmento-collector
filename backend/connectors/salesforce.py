import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "salesforce"
ACCOUNTS_SOURCE = "salesforce_accounts"
CONTACTS_SOURCE = "salesforce_contacts"
LEADS_SOURCE = "salesforce_leads"
API_VERSION = "v58.0"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[SALESFORCE] {message}", flush=True)


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


def save_config(uid: str, client_id: str, client_secret: str, instance_url: str):
    config = {
        "client_id": client_id.strip(),
        "client_secret": client_secret.strip(),
        "instance_url": instance_url.strip(),
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


def _get_access_token(client_id: str, client_secret: str, instance_url: str) -> str:
    """Exchange OAuth credentials for access token"""
    url = f"{instance_url}/services/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    response = requests.post(url, data=payload, timeout=30)
    if response.status_code != 200:
        raise Exception(f"OAuth failed: {response.text}")
    data = response.json()
    return data["access_token"]


def _request(method: str, url: str, access_token: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers["Authorization"] = f"Bearer {access_token}"
    headers["Content-Type"] = "application/json"

    for attempt in range(retries):
        response = requests.request(method, url, headers=headers, timeout=40, **kwargs)

        if response.status_code == 429:
            wait_s = min(2 ** attempt, 15)
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
        raise Exception(f"Salesforce API error {response.status_code}: {response.text}")

    return response.json() if response.text else {}


def _fetch_sobject_records(instance_url: str, access_token: str, object_type: str, last_sync_at: datetime.datetime | None = None) -> list[dict]:
    """Fetch records for a Salesforce object (Account, Contact, Lead)"""
    results = []
    
    # Build SOQL query
    soql = f"SELECT FIELDS(ALL) FROM {object_type} LIMIT 2000"
    if last_sync_at:
        # Incremental sync
        timestamp_str = last_sync_at.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        soql = f"SELECT FIELDS(ALL) FROM {object_type} WHERE SystemModstamp > {timestamp_str} LIMIT 2000"
    
    url = f"{instance_url}/services/data/{API_VERSION}/query"
    params = {"q": soql}
    
    data = _request("GET", url, access_token, params=params)
    results.extend(data.get("records", []))
    
    # Handle pagination
    while data.get("nextRecordsUrl"):
        next_url = f"{instance_url}{data['nextRecordsUrl']}"
        data = _request("GET", next_url, access_token)
        results.extend(data.get("records", []))
    
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


def connect_salesforce(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Salesforce not configured for this user"}

    try:
        access_token = _get_access_token(
            cfg["client_id"], 
            cfg["client_secret"],
            cfg["instance_url"]
        )
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} instance={cfg['instance_url']}")
    return {
        "status": "success",
        "instance_url": cfg["instance_url"],
    }


def sync_salesforce(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Salesforce not configured"}

    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        access_token = _get_access_token(
            cfg["client_id"], 
            cfg["client_secret"],
            cfg["instance_url"]
        )
        
        accounts = _fetch_sobject_records(cfg["instance_url"], access_token, "Account", last_sync_at)
        contacts = _fetch_sobject_records(cfg["instance_url"], access_token, "Contact", last_sync_at)
        leads = _fetch_sobject_records(cfg["instance_url"], access_token, "Lead", last_sync_at)
        
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Accounts
    account_rows = []
    for account in accounts:
        account_rows.append({
            "uid": uid,
            "source": ACCOUNTS_SOURCE,
            "account_id": account.get("Id"),
            "name": account.get("Name"),
            "type": account.get("Type"),
            "industry": account.get("Industry"),
            "annual_revenue": account.get("AnnualRevenue"),
            "phone": account.get("Phone"),
            "website": account.get("Website"),
            "billing_city": account.get("BillingCity"),
            "billing_country": account.get("BillingCountry"),
            "created_date": account.get("CreatedDate"),
            "last_modified_date": account.get("LastModifiedDate"),
            "data_json": json.dumps(account, default=str),
            "raw_json": json.dumps(account, default=str),
            "fetched_at": fetched_at,
        })

    # Process Contacts
    contact_rows = []
    for contact in contacts:
        contact_rows.append({
            "uid": uid,
            "source": CONTACTS_SOURCE,
            "contact_id": contact.get("Id"),
            "account_id": contact.get("AccountId"),
            "first_name": contact.get("FirstName"),
            "last_name": contact.get("LastName"),
            "email": contact.get("Email"),
            "phone": contact.get("Phone"),
            "title": contact.get("Title"),
            "department": contact.get("Department"),
            "mailing_city": contact.get("MailingCity"),
            "mailing_country": contact.get("MailingCountry"),
            "created_date": contact.get("CreatedDate"),
            "last_modified_date": contact.get("LastModifiedDate"),
            "data_json": json.dumps(contact, default=str),
            "raw_json": json.dumps(contact, default=str),
            "fetched_at": fetched_at,
        })

    # Process Leads
    lead_rows = []
    for lead in leads:
        lead_rows.append({
            "uid": uid,
            "source": LEADS_SOURCE,
            "lead_id": lead.get("Id"),
            "first_name": lead.get("FirstName"),
            "last_name": lead.get("LastName"),
            "email": lead.get("Email"),
            "phone": lead.get("Phone"),
            "company": lead.get("Company"),
            "title": lead.get("Title"),
            "status": lead.get("Status"),
            "lead_source": lead.get("LeadSource"),
            "city": lead.get("City"),
            "country": lead.get("Country"),
            "created_date": lead.get("CreatedDate"),
            "last_modified_date": lead.get("LastModifiedDate"),
            "data_json": json.dumps(lead, default=str),
            "raw_json": json.dumps(lead, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(account_rows) + len(contact_rows) + len(lead_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, ACCOUNTS_SOURCE, account_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, CONTACTS_SOURCE, contact_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, LEADS_SOURCE, lead_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "accounts_found": len(account_rows),
        "contacts_found": len(contact_rows),
        "leads_found": len(lead_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_salesforce(uid: str) -> dict:
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
