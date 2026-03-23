import datetime
import json
import sqlite3
import time

from backend.destinations.destination_router import push_to_destination
from backend.security.crypto import encrypt_value
from backend.security.secure_fetch import fetchone_secure

DB = "identity.db"
SOURCE = "kafka"
TOPICS_SOURCE = "kafka_topics"
PARTITIONS_SOURCE = "kafka_partitions"
MESSAGES_SOURCE = "kafka_messages"


def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(message: str):
    print(f"[KAFKA] {message}")


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


def save_config(uid: str, bootstrap_servers: str):
    config = {"bootstrap_servers": bootstrap_servers.strip()}

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


def _fetch_kafka_metadata(bootstrap_servers: str):
    """
    Mock metadata fetch for Kafka
    In production, use kafka-python library
    """
    try:
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic
        
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers.split(","),
            client_id='segmento_collector'
        )
        
        topics = admin_client.list_topics()
        consumer_groups = admin_client.list_consumer_groups()
        
        metadata = {
            "topics": list(topics),
            "consumer_groups": [cg[0] for cg in consumer_groups],
        }
        
        admin_client.close()
        return metadata
        
    except ImportError:
        # Mock data if kafka-python not installed
        _log("kafka-python not installed, using mock data")
        return {
            "topics": ["sample_topic_1", "sample_topic_2"],
            "consumer_groups": ["sample_group_1"],
        }
    except Exception as e:
        _log(f"Failed to fetch Kafka metadata: {e}")
        raise


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


def connect_kafka(uid: str) -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Kafka not configured for this user"}

    try:
        metadata = _fetch_kafka_metadata(cfg["bootstrap_servers"])
    except Exception as exc:
        _log(f"Connection failed for uid={uid}: {exc}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    _log(f"Connected uid={uid}")
    return {
        "status": "success",
        "topic_count": len(metadata.get("topics", [])),
    }


def sync_kafka(uid: str, sync_type: str = "incremental") -> dict:
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "Kafka not configured"}

    bootstrap_servers = cfg["bootstrap_servers"]
    state = get_state(uid)
    last_sync_at = _parse_dt(state.get("last_sync_at")) if sync_type == "incremental" else None

    try:
        metadata = _fetch_kafka_metadata(bootstrap_servers)
    except Exception as exc:
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": str(exc)}

    dest_cfg = _get_active_destination(uid)
    fetched_at = _iso_now() + "Z"

    total_rows_found = 0
    total_rows_pushed = 0

    # Process Topics
    topic_rows = []
    partition_rows = []
    for topic_name in metadata.get("topics", []):
        topic_rows.append({
            "uid": uid,
            "source": TOPICS_SOURCE,
            "topic_name": topic_name,
            "partition_count": 1,  # Mock value
            "replication_factor": 1,  # Mock value
            "data_json": json.dumps({"name": topic_name}, default=str),
            "raw_json": json.dumps({"name": topic_name}, default=str),
            "fetched_at": fetched_at,
        })
        
        # Mock partition data
        partition_rows.append({
            "uid": uid,
            "source": PARTITIONS_SOURCE,
            "topic_name": topic_name,
            "partition_id": 0,
            "leader": 1,
            "replicas": json.dumps([1]),
            "isr": json.dumps([1]),
            "data_json": json.dumps({"topic": topic_name, "partition": 0}, default=str),
            "raw_json": json.dumps({"topic": topic_name, "partition": 0}, default=str),
            "fetched_at": fetched_at,
        })

    # Process Consumer Groups
    consumer_group_rows = []
    for group_name in metadata.get("consumer_groups", []):
        consumer_group_rows.append({
            "uid": uid,
            "source": MESSAGES_SOURCE,
            "group_id": group_name,
            "state": "active",
            "members": 1,  # Mock value
            "data_json": json.dumps({"group_id": group_name}, default=str),
            "raw_json": json.dumps({"group_id": group_name}, default=str),
            "fetched_at": fetched_at,
        })

    total_rows_found += len(topic_rows) + len(partition_rows) + len(consumer_group_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, TOPICS_SOURCE, topic_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, PARTITIONS_SOURCE, partition_rows)
    total_rows_pushed += _push_rows(dest_cfg, SOURCE, MESSAGES_SOURCE, consumer_group_rows)

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")
    save_state(uid, {"last_sync_at": _iso_now()})

    result = {
        "status": "success",
        "topics_found": len(topic_rows),
        "partitions_found": len(partition_rows),
        "consumer_groups_found": len(consumer_group_rows),
        "rows_found": total_rows_found,
        "rows_pushed": total_rows_pushed,
        "sync_type": sync_type,
    }
    if not dest_cfg:
        result["message"] = "No active destination configured"
    return result


def disconnect_kafka(uid: str) -> dict:
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
