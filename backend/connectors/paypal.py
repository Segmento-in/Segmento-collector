import datetime
import json
import sqlite3
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "paypal"
TRANSACTIONS_SOURCE = "paypal_transactions"
PAYMENTS_SOURCE = "paypal_payments"
API_BASE = "https://api.paypal.com"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[PAYPAL] {message}", flush=True)


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


def save_config(uid: str, client_id: str, client_secret: str, use_sandbox: bool = False):
    config = {
        "client_id": client_id.strip(),
        "client_secret": client_secret.strip(),
        "use_sandbox": use_sandbox,
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
    _log(f"Config saved for uid={uid} sandbox={use_sandbox}")


def _get_access_token(client_id: str, client_secret: str, use_sandbox: bool = False) -> str:
    """Get PayPal OAuth access token"""
    base_url = "https://api.sandbox.paypal.com" if use_sandbox else "https://api.paypal.com"
    url = f"{base_url}/v1/oauth2/token"
    
    headers = {
        "Accept": "application/json",
        "Accept-Language": "en_US",
    }
    
    data = {"grant_type": "client_credentials"}
    auth = (client_id, client_secret)
    
    response = requests.post(url, auth=auth, headers=headers, data=data, timeout=30)
    
    if response.status_code != 200:
        raise Exception(f"PayPal OAuth failed: {response.text}")
    
    result = response.json()
    return result["access_token"]


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
        raise Exception(f"PayPal API error {response.status_code}: {detail}")

    return response.json() if response.text else {}


def _fetch_transactions(access_token: str, use_sandbox: bool, last_sync_at: datetime.datetime | None = None) -> list[dict]:
    """Fetch PayPal transaction history"""
    base_url = "https://api.sandbox.paypal.com" if use_sandbox else "https://api.paypal.com"
    url = f"{base_url}/v1/reporting/transactions"
    
    # Build date range for query
    end_date = datetime.datetime.now(datetime.UTC)
    start_date = last_sync_at if last_sync_at else (end_date - datetime.timedelta(days=30))
    
    params = {
        "start_date": start_date.strftime("%Y-%m-%dT%H:%M:%S-0000"),
        "end_date": end_date.strftime("%Y-%m-%dT%H:%M:%S-0000"),
        "fields": "all",
        "page_size": 500,
        "page": 1,
    }
    
    all_transactions = []
    
    while True:
        data = _request("GET", url, access_token, params=params)
        
        transactions = data.get("transaction_details", [])
        if not transactions:
            break
            
        all_transactions.extend(transactions)
        
        # Check for more pages
        total_pages = data.get("total_pages", 1)
        if params["page"] >= total_pages:
            break
            
        params["page"] += 1
    
    return all_transactions


def _fetch_payments(access_token: str, use_sandbox: bool, last_sync_at: datetime.datetime | None = None) -> list[dict]:
    """Fetch PayPal payment history"""
    base_url = "https://api.sandbox.paypal.com" if use_sandbox else "https://api.paypal.com"
    url = f"{base_url}/v1/payments/payment"
    
    params = {
        "count": 100,
        "start_index": 0,
    }
    
    all_payments = []
    
    while True:
        data = _request("GET", url, access_token, params=params)
        
        payments = data.get("payments", [])
        if not payments:
            break
            
        # Filter by last_sync_at if incremental
        if last_sync_at:
            payments = [
                p for p in payments
                if (_parse_dt(p.get("update_time")) or datetime.datetime.min.replace(tzinfo=datetime.UTC)) > last_sync_at
            ]
        
        all_payments.extend(payments)
        
        # Check if there are more payments
        if len(payments) < params["count"]:
            break
            
        params["start_index"] += params["count"]
        
        # Safety limit to prevent infinite loops
        if params["start_index"] > 1000:
            break
    
    return all_payments


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


def connect_paypal(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "PayPal not configured for this user"}

    try:
        access_token = _get_access_token(
            cfg["client_id"],
            cfg["client_secret"],
            cfg.get("use_sandbox", False)
        )
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    mode = "sandbox" if cfg.get("use_sandbox") else "live"
    _log(f"Connected uid={uid} mode={mode}")
    return {
        "status": "success",
        "mode": mode,
    }


def sync_paypal(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "PayPal not configured"}

    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        access_token = _get_access_token(
            cfg["client_id"],
            cfg["client_secret"],
            cfg.get("use_sandbox", False)
        )
        
        transactions = _fetch_transactions(access_token, cfg.get("use_sandbox", False), last_sync_at)
        payments = _fetch_payments(access_token, cfg.get("use_sandbox", False), last_sync_at)
        
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Transactions
    transaction_rows = []
    for txn in transactions:
        transaction_info = txn.get("transaction_info", {})
        transaction_rows.append({
            "uid": uid,
            "source": TRANSACTIONS_SOURCE,
            "transaction_id": transaction_info.get("transaction_id"),
            "paypal_account_id": transaction_info.get("paypal_account_id"),
            "transaction_event_code": transaction_info.get("transaction_event_code"),
            "transaction_status": transaction_info.get("transaction_status"),
            "transaction_amount": transaction_info.get("transaction_amount", {}).get("value"),
            "currency_code": transaction_info.get("transaction_amount", {}).get("currency_code"),
            "fee_amount": transaction_info.get("fee_amount", {}).get("value"),
            "initiation_date": transaction_info.get("transaction_initiation_date"),
            "updated_date": transaction_info.get("transaction_updated_date"),
            "data_json": json.dumps(txn, default=str),
            "raw_json": json.dumps(txn, default=str),
            "fetched_at": fetched_at,
        })

    # Process Payments
    payment_rows = []
    for payment in payments:
        payment_rows.append({
            "uid": uid,
            "source": PAYMENTS_SOURCE,
            "payment_id": payment.get("id"),
            "intent": payment.get("intent"),
            "state": payment.get("state"),
            "cart": payment.get("cart"),
            "payer_email": (payment.get("payer") or {}).get("payer_info", {}).get("email"),
            "payer_id": (payment.get("payer") or {}).get("payer_info", {}).get("payer_id"),
            "create_time": payment.get("create_time"),
            "update_time": payment.get("update_time"),
            "data_json": json.dumps(payment, default=str),
            "raw_json": json.dumps(payment, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(transaction_rows) + len(payment_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, TRANSACTIONS_SOURCE, transaction_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PAYMENTS_SOURCE, payment_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "transactions_found": len(transaction_rows),
        "payments_found": len(payment_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_paypal(uid: str) -> dict:
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
