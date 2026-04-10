import datetime
import json
import sqlite3
import os
import time

import requests

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = os.getenv("DB_PATH", "identity.db")
SOURCE = "pinecone"
INDEXES_SOURCE = "pinecone_indexes"
NAMESPACES_SOURCE = "pinecone_namespaces"
VECTORS_SOURCE = "pinecone_vectors"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[PINECONE] {message}", flush=True)


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
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def _iso_now():
    return datetime.datetime.utcnow().isoformat()


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
    try:
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
    except Exception as e:
        pass


def save_config(uid: str, api_key: str, environment: str):
    config = {
        "api_key": api_key.strip(),
        "environment": environment.strip(),
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


def _get_headers(api_key: str) -> dict:
    return {
        "Api-Key": api_key,
        "Content-Type": "application/json",
    }


def _controller_url(environment: str) -> str:
    return f"https://controller.{environment}.pinecone.io"


def _request(method: str, url: str, api_key: str, retries: int = 4, **kwargs):
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_get_headers(api_key))

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
        raise Exception(f"Pinecone API error {response.status_code}: {detail}")

    if response.status_code == 204:
        return {}
    return response.json()


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


def connect_pinecone(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Pinecone not configured for this user"}

    base_url = _controller_url(cfg["environment"])

    try:
        data = _request("GET", f"{base_url}/databases", cfg["api_key"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    indexes = data if isinstance(data, list) else (data.get("databases") or [])
    index_count = len(indexes)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid} environment={cfg['environment']} indexes={index_count}")
    return {
        "status": "success",
        "api_key": _mask_token(cfg.get("api_key")),
        "environment": cfg.get("environment"),
        "index_count": index_count,
    }


def sync_pinecone(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Pinecone not configured"}

    api_key = cfg["api_key"]
    environment = cfg["environment"]
    base_url = _controller_url(environment)

    try:
        raw_indexes_data = _request("GET", f"{base_url}/databases", api_key)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    raw_indexes = raw_indexes_data if isinstance(raw_indexes_data, list) else (raw_indexes_data.get("databases") or [])

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    index_rows = []
    namespace_rows = []
    vector_rows = []

    for index_name in raw_indexes:
        # Fetch index description
        try:
            idx = _request("GET", f"{base_url}/databases/{index_name}", api_key)
        except Exception as exc:
            _log(f"Could not describe index {index_name}: {exc}")
            idx = {"name": index_name}

        status_info = idx.get("status") or {}
        database_info = idx.get("database") or idx

        index_rows.append({
            "uid": uid,
            "source": INDEXES_SOURCE,
            "index_name": index_name,
            "environment": environment,
            "dimension": database_info.get("dimension"),
            "metric": database_info.get("metric"),
            "replicas": database_info.get("replicas"),
            "shards": database_info.get("shards"),
            "pods": database_info.get("pods"),
            "pod_type": database_info.get("pod_type"),
            "ready": bool(status_info.get("ready")),
            "state": status_info.get("state"),
            "host": status_info.get("host"),
            "port": status_info.get("port"),
            "data_json": json.dumps(idx, default=str),
            "raw_json": json.dumps(idx, default=str),
            "fetched_at": fetched_at,
        })

        # Fetch namespace stats via index host
        index_host = status_info.get("host")
        if index_host:
            try:
                index_url = f"https://{index_host}"
                stats = _request("GET", f"{index_url}/describe_index_stats", api_key)
                namespaces = stats.get("namespaces") or {}

                for ns_name, ns_stats in namespaces.items():
                    namespace_rows.append({
                        "uid": uid,
                        "source": NAMESPACES_SOURCE,
                        "index_name": index_name,
                        "namespace": ns_name,
                        "vector_count": ns_stats.get("vectorCount"),
                        "data_json": json.dumps({"namespace": ns_name, **ns_stats}, default=str),
                        "raw_json": json.dumps({"namespace": ns_name, **ns_stats}, default=str),
                        "fetched_at": fetched_at,
                    })

                # Sample up to 100 vectors from the default namespace
                try:
                    list_data = _request(
                        "GET",
                        f"{index_url}/vectors/list",
                        api_key,
                        params={"limit": 100},
                    )
                    vector_ids = list_data.get("vectors") or []
                    if vector_ids:
                        fetch_data = _request(
                            "GET",
                            f"{index_url}/vectors/fetch",
                            api_key,
                            params={"ids": [v.get("id") for v in vector_ids if v.get("id")]},
                        )
                        fetched_vectors = (fetch_data.get("vectors") or {})
                        for vec_id, vec in fetched_vectors.items():
                            vector_rows.append({
                                "uid": uid,
                                "source": VECTORS_SOURCE,
                                "index_name": index_name,
                                "vector_id": vec_id,
                                "namespace": fetch_data.get("namespace") or "",
                                "dimension": len(vec.get("values") or []),
                                "metadata_json": json.dumps(vec.get("metadata") or {}, default=str),
                                "data_json": json.dumps(vec, default=str),
                                "raw_json": json.dumps(vec, default=str),
                                "fetched_at": fetched_at,
                            })
                except Exception as exc:
                    _log(f"Could not sample vectors for index {index_name}: {exc}")

            except Exception as exc:
                _log(f"Could not fetch stats for index {index_name}: {exc}")

    total_rows_found = len(index_rows) + len(namespace_rows) + len(vector_rows)
    total_rows_pushed = 0
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, INDEXES_SOURCE, index_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, NAMESPACES_SOURCE, namespace_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, VECTORS_SOURCE, vector_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "indexes_found": len(index_rows),
        "namespaces_found": len(namespace_rows),
        "vectors_found": len(vector_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_pinecone(uid: str) -> dict:

    try:
        _set_connection_enabled(uid, False)
        _update_status(uid, "disconnected")
        _log(f"Disconnected uid={uid}")
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

