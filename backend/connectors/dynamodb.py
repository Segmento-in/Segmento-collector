import datetime
import hashlib
import json
import sqlite3
from decimal import Decimal

import boto3
from boto3.dynamodb.types import Binary

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "dynamodb"
TABLES_SOURCE = "dynamodb_tables"
DATA_SOURCE = "dynamodb_data"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[DYNAMODB] {message}", flush=True)


def _mask_access_key(access_key: str | None) -> str | None:
    if not access_key:
        return None
    if len(access_key) <= 4:
        return "*" * len(access_key)
    return f"{access_key[:4]}{'*' * max(len(access_key) - 8, 4)}{access_key[-4:]}"


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


def save_config(uid: str, access_key: str, secret_key: str, region: str):
    config = {
        "access_key": access_key.strip(),
        "secret_key": secret_key,
        "region": region.strip(),
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
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}, region={config['region']}")


def _build_session(cfg: dict):
    return boto3.Session(
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=cfg["region"],
    )


def _build_clients(cfg: dict):
    session = _build_session(cfg)
    client = session.client("dynamodb")
    resource = session.resource("dynamodb")
    return client, resource


def _list_tables(client) -> list[str]:
    paginator = client.get_paginator("list_tables")
    tables = []
    for page in paginator.paginate():
        tables.extend(page.get("TableNames", []))
    return tables


def _normalize_value(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, Binary):
        return bytes(value).hex()
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    if isinstance(value, memoryview):
        return value.tobytes().hex()
    if isinstance(value, set):
        return [_normalize_value(v) for v in sorted(value, key=lambda item: str(item))]
    if isinstance(value, list):
        return [_normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _normalize_value(v) for k, v in value.items()}
    return value


def _normalize_item(item: dict) -> dict:
    return {str(key): _normalize_value(value) for key, value in item.items()}


def _record_id(item: dict, key_names: list[str]) -> str:
    if key_names:
        parts = []
        for key_name in key_names:
            if key_name in item:
                parts.append(f"{key_name}={item[key_name]}")
        if parts:
            return "|".join(parts)

    payload = json.dumps(item, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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


def connect_dynamodb(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "DynamoDB not configured for this user"}

    try:
        client, _ = _build_clients(cfg)
        response = client.list_tables(Limit=1)
        table_count = len(response.get("TableNames", []))
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} region={cfg['region']}")
    return {
        "status": "success",
        "region": cfg["region"],
        "access_key": _mask_access_key(cfg.get("access_key")),
        "tables_visible": table_count,
    }


def sync_dynamodb(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "DynamoDB not configured"}

    try:
        client, resource = _build_clients(cfg)
        tables = _list_tables(client)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    now = datetime.datetime.now(datetime.UTC).isoformat() + "Z"

    _log(f"uid={uid} discovered {len(tables)} tables in region={cfg['region']}: {tables}")
    if dest_cfg:
        _log(f"Active destination found for uid={uid}: type={dest_cfg.get('type')}")
    else:
        _log(f"No active destination found for uid={uid} source={SOURCE}")

    tables_processed = 0
    total_rows_found = 0
    total_rows_pushed = 0

    for table_name in tables:
        try:
            description = client.describe_table(TableName=table_name)["Table"]
            key_names = [key["AttributeName"] for key in description.get("KeySchema", [])]
            table_row = {
                "uid": uid,
                "source": TABLES_SOURCE,
                "table_name": table_name,
                "table_arn": description.get("TableArn"),
                "table_status": description.get("TableStatus"),
                "billing_mode": (
                    description.get("BillingModeSummary", {}).get("BillingMode")
                    or "PROVISIONED"
                ),
                "item_count": description.get("ItemCount", 0),
                "size_bytes": description.get("TableSizeBytes", 0),
                "region": cfg["region"],
                "fetched_at": now,
            }

            total_rows_pushed += _push_rows(
                dest_cfg,
                SOURCE,
                TABLES_SOURCE,
                [table_row],
            )

            table = resource.Table(table_name)
            scan_kwargs = {}
            table_rows_found = 0
            table_rows_pushed = 0
            pages_scanned = 0

            while True:
                response = table.scan(**scan_kwargs)
                items = response.get("Items", [])
                pages_scanned += 1

                _log(
                    f"table={table_name} page={pages_scanned} "
                    f"items_scanned={len(items)}"
                )

                batch = []
                for item in items:
                    normalized = _normalize_item(item)
                    batch.append(
                        {
                            "uid": uid,
                            "source": DATA_SOURCE,
                            "table_name": table_name,
                            "record_id": _record_id(normalized, key_names),
                            "data_json": json.dumps(normalized, default=str),
                            "raw_json": json.dumps(normalized, default=str),
                            "fetched_at": now,
                        }
                    )

                table_rows_found += len(batch)

                pushed = _push_rows(
                    dest_cfg,
                    SOURCE,
                    f"{DATA_SOURCE}:{table_name}",
                    batch,
                )
                table_rows_pushed += pushed
                total_rows_pushed += pushed

                last_key = response.get("LastEvaluatedKey")
                if not last_key:
                    break
                scan_kwargs["ExclusiveStartKey"] = last_key

            total_rows_found += table_rows_found
            tables_processed += 1
            _log(
                f"table={table_name} pages_scanned={pages_scanned} "
                f"items_scanned_total={table_rows_found} rows_pushed={table_rows_pushed}"
            )

        except Exception as exc:
            _log(f"Failed extracting table '{table_name}': {exc}")
            continue

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")

    result = {
        "status": "success",
        "tables_processed": tables_processed,
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_dynamodb(uid: str) -> dict:
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
    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return {
        "type": row[0],
        "host": row[1],
        "port": row[2],
        "username": row[3],
        "password": row[4],
        "database_name": row[5],
    }
