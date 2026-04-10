import datetime
import json
import sqlite3
import os

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure


DB = os.getenv("DB_PATH", "identity.db")
SOURCE = "ebay"
ORDERS_SOURCE = "ebay_orders"
LISTINGS_SOURCE = "ebay_listings"
CUSTOMERS_SOURCE = "ebay_customers"
API_BASE = "https://api.ebay.com"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _iso_now():
    return datetime.datetime.utcnow().isoformat()


def _parse_dt(value):
    if not value:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
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
    try:
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
    except Exception as e:
        pass


def save_config(uid: str, payload: dict):
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO connector_configs
        (uid, connector, config_json, status, created_at)
        VALUES (?, ?, ?, 'pending', ?)
        """,
        (uid, SOURCE, encrypt_value(json.dumps({"access_token": (payload.get("access_token") or "").strip()})), _iso_now()),
    )
    con.commit()
    con.close()


def _request(path: str, token: str, params=None) -> dict:
    response = requests.get(
        f"{API_BASE}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        params=params,
        timeout=10,
    )
    if response.status_code >= 400:
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise Exception(f"eBay API error {response.status_code}: {detail}")
    return response.json() if response.text else {}


def _filter_incremental(rows: list[dict], last_sync_at, fields: tuple[str, ...]):
    if not last_sync_at:
        return rows
    filtered = []
    for row in rows:
        updated_at = None
        for field in fields:
            updated_at = _parse_dt(row.get(field))
            if updated_at:
                break
        if not updated_at or updated_at > last_sync_at:
            filtered.append(row)
    return filtered


def _push_rows(dest_cfg: dict | None, route_source: str, label: str, rows: list[dict]) -> int:
    if not dest_cfg or not rows:
        return 0
    return push_to_destination(dest_cfg, route_source, rows)


def connect_ebay(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "eBay not configured"}
    try:
        orders = _request("/sell/fulfillment/v1/order", cfg["access_token"], params={"limit": 1}).get("orders", [])
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}
    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    return {
        "status": "success",
        "access_token": _mask_token(cfg.get("access_token")),
        "orders_visible": len(orders),
    }


def sync_ebay(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "eBay not configured"}
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None
    try:
        orders = _request("/sell/fulfillment/v1/order", cfg["access_token"], params={"limit": 200}).get("orders", [])
        listings = _request("/sell/inventory/v1/inventory_item", cfg["access_token"], params={"limit": 200}).get("inventoryItems", [])
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    orders = _filter_incremental(orders, last_sync_at, ("creationDate", "lastModifiedDate"))
    listings = _filter_incremental(listings, last_sync_at, ("availability",))

    fetched_at = _iso_now() + "Z"
    dest_cfg = _get_active_destination(uid)

    customers_map = {}
    for order in orders:
        buyer = order.get("buyer") or {}
        username = buyer.get("username")
        if not username:
            continue
        customers_map[username] = {
            "uid": uid,
            "source": CUSTOMERS_SOURCE,
            "customer_id": username,
            "username": username,
            "email": buyer.get("email"),
            "order_id": order.get("orderId"),
            "raw_json": json.dumps({"buyer": buyer, "orderId": order.get("orderId")}, default=str),
            "fetched_at": fetched_at,
        }

    order_rows = [{
        "uid": uid,
        "source": ORDERS_SOURCE,
        "order_id": item.get("orderId"),
        "legacy_order_id": item.get("legacyOrderId"),
        "order_creation_date": item.get("creationDate"),
        "order_last_modified_date": item.get("lastModifiedDate"),
        "order_fulfillment_status": item.get("orderFulfillmentStatus"),
        "order_payment_status": item.get("orderPaymentStatus"),
        "buyer_username": (item.get("buyer") or {}).get("username"),
        "pricing_summary": json.dumps(item.get("pricingSummary") or {}, default=str),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in orders]

    listing_rows = [{
        "uid": uid,
        "source": LISTINGS_SOURCE,
        "sku": item.get("sku"),
        "title": ((item.get("product") or {}).get("title")),
        "condition": item.get("condition"),
        "availability": json.dumps(item.get("availability") or {}, default=str),
        "product": json.dumps(item.get("product") or {}, default=str),
        "raw_json": json.dumps(item, default=str),
        "fetched_at": fetched_at,
    } for item in listings]

    customer_rows = list(customers_map.values())
    rows_found = len(order_rows) + len(listing_rows) + len(customer_rows)
    rows_pushed = 0
    rows_pushed += _push_rows(dest_cfg, SOURCE, ORDERS_SOURCE, order_rows)
    rows_pushed += _push_rows(dest_cfg, SOURCE, LISTINGS_SOURCE, listing_rows)
    rows_pushed += _push_rows(dest_cfg, SOURCE, CUSTOMERS_SOURCE, customer_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "orders_found": len(order_rows),
        "listings_found": len(listing_rows),
        "customers_found": len(customer_rows),
        "rows_found": rows_found,
        "rows_pushed": rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_ebay(uid: str) -> dict:

    try:
        _set_connection_enabled(uid, False)
        _update_status(uid, "disconnected")
        return {"status": "success"}
    
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"status": "error", "message": str(e)}

def _get_active_destination(uid: str) -> dict | None:
    con = get_db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT dest_type, host, port, username, password, database_name, format
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
        "type": row.get("dest_type"),
        "host": row.get("host"),
        "port": row.get("port"),
        "username": row.get("username"),
        "password": row.get("password"),
        "database_name": row.get("database_name"),
        "format": row.get("format")
    }

