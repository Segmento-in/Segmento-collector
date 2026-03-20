import json
import sqlite3
import datetime

from backend.security.secure_fetch import fetchone_secure
from backend.security.crypto import encrypt_value
from backend.destinations.destination_router import push_to_destination

DB = "identity.db"
SOURCE = "aws_rds"

# MySQL-family engines
MYSQL_ENGINES = {"mysql", "mariadb", "aurora_mysql", "aurora-mysql"}
# Postgres-family engines
POSTGRES_ENGINES = {"postgres", "postgresql", "aurora_postgres", "aurora-postgres"}

BATCH_SIZE = 500


# ──────────────────────────────────────────────
# DB helpers
# ──────────────────────────────────────────────

def get_db():
    con = sqlite3.connect(DB, timeout=60, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _log(msg: str):
    print(f"[AWS-RDS] {msg}", flush=True)


# ──────────────────────────────────────────────
# Config helpers
# ──────────────────────────────────────────────

def _get_config(uid: str) -> dict | None:
    """
    Load connector configuration from connector_configs.
    Expected config_json:
    {
        "engine":   "mysql" | "postgres" | "mariadb" | "aurora_mysql" | "aurora_postgres",
        "host":     "...",
        "port":     3306,
        "database": "...",
        "username": "...",
        "password": "..."
    }
    """
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

    cur.execute("""
        UPDATE google_connections
        SET enabled = ?
        WHERE uid = ? AND source = ?
    """, (1 if enabled else 0, uid, SOURCE))

    # if row does not exist yet, insert it
    if cur.rowcount == 0:
        cur.execute("""
            INSERT INTO google_connections (uid, source, enabled)
            VALUES (?, ?, ?)
        """, (uid, SOURCE, 1 if enabled else 0))

    con.commit()
    con.close()

def save_config(uid: str, engine: str, host: str, port: int,
                database: str, username: str, password: str):
    """
    Persist encrypted RDS configuration into connector_configs.
    Called from the api_server save_app route before connect.
    """
    config = {
        "engine":   engine.lower().strip(),
        "host":     host.strip(),
        "port":     int(port),
        "database": database.strip(),
        "username": username.strip(),
        "password": password,
    }
    config_json = json.dumps(config)

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
            encrypt_value(config_json),
            datetime.datetime.now(datetime.UTC).isoformat(),
        ),
    )
    con.commit()
    con.close()
    _log(f"Config saved for uid={uid}, engine={engine}, host={host}")


# ──────────────────────────────────────────────
# Driver helpers
# ──────────────────────────────────────────────

def _engine_family(engine: str) -> str:
    """Return 'mysql' or 'postgres' for any supported engine string."""
    e = engine.lower().strip()
    if e in MYSQL_ENGINES:
        return "mysql"
    if e in POSTGRES_ENGINES:
        return "postgres"
    raise ValueError(
        f"Unsupported engine '{engine}'. "
        f"Supported: mysql, mariadb, aurora_mysql, postgres, aurora_postgres"
    )


def _open_connection(cfg: dict):
    """
    Open and return a live DB connection (pymysql or psycopg2)
    plus its family string.
    """
    family = _engine_family(cfg["engine"])
    host = cfg["host"]
    port = int(cfg.get("port") or (3306 if family == "mysql" else 5432))
    database = cfg["database"]
    username = cfg["username"]
    password = cfg.get("password", "")

    if family == "mysql":
        import pymysql
        conn = pymysql.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            connect_timeout=10,
            read_timeout=60,
            write_timeout=60,
            charset="utf8mb4",
            autocommit=True,
        )
    else:
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            connect_timeout=10,
        )

    return conn, family


# ──────────────────────────────────────────────
# Table discovery
# ──────────────────────────────────────────────

def _list_tables(conn, family: str, database: str) -> list[str]:
    """Return a list of user-visible table names."""
    cur = conn.cursor()

    if family == "mysql":
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
            (database,),
        )
    else:
        cur.execute(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
            "ORDER BY tablename"
        )

    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


# ──────────────────────────────────────────────
# Row normalisation
# ──────────────────────────────────────────────

def _normalize_row(columns: list[str], values: tuple) -> dict:
    """
    Convert a raw DB row into a fully JSON-serialisable dict.
    Handles datetime, date, Decimal, bytes, etc.
    """
    import decimal

    record = {}
    for col, val in zip(columns, values):
        if isinstance(val, (datetime.datetime, datetime.date)):
            record[col] = val.isoformat()
        elif isinstance(val, decimal.Decimal):
            record[col] = float(val)
        elif isinstance(val, (bytes, bytearray, memoryview)):
            record[col] = val.tobytes().decode("utf-8", errors="replace") if isinstance(val, memoryview) else val.decode("utf-8", errors="replace")
        else:
            record[col] = val
    return record


# ──────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────

def connect_rds(uid: str) -> dict:
    """
    Validate RDS credentials and mark the connector as connected.

    Steps:
    1. Load config from connector_configs
    2. Open a live connection to the RDS instance
    3. Run a lightweight ping query
    4. Update status + enable in google_connections on success
    """
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "AWS RDS not configured for this user"}

    try:
        conn, family = _open_connection(cfg)
    except Exception as e:
        _log(f"Connection failed for uid={uid}: {e}")
        _update_status(uid, "error")
        return {"status": "error", "message": str(e)}

    # Lightweight ping
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
    except Exception as e:
        _log(f"Ping failed for uid={uid}: {e}")
        _update_status(uid, "error")
        _set_connection_enabled(uid, False)
        return {"status": "error", "message": f"Connection ping failed: {e}"}

    _set_connection_enabled(uid, True)
    _update_status(uid, "connected")

    _log(f"Connected uid={uid} engine={cfg['engine']} host={cfg['host']}")
    return {
        "status": "success",
        "engine": cfg["engine"],
        "host": cfg["host"],
        "database": cfg["database"],
    }


def sync_rds(uid: str, sync_type: str = "incremental") -> dict:
    """
    Extract all tables from the configured RDS instance and push
    every batch of rows to the active destination via push_to_destination().

    Extraction flow:
        discover tables
        for each table:
            read column names
            fetch rows in batches of BATCH_SIZE
            normalise each row to a dict
            push batch to destination
    """
    cfg = _get_config(uid)
    if not cfg:
        return {"status": "error", "message": "AWS RDS not configured"}

    try:
        conn, family = _open_connection(cfg)
    except Exception as e:
        _update_status(uid, "error")
        return {"status": "error", "message": str(e)}

    # Discover tables
    try:
        tables = _list_tables(conn, family, cfg["database"])
    except Exception as e:
        conn.close()
        return {"status": "error", "message": f"Table discovery failed: {e}"}

    _log(f"uid={uid} discovered {len(tables)} tables")

    # Fetch active destination
    dest_cfg = _get_active_destination(uid)

    tables_processed = 0
    total_rows_found = 0
    total_rows_pushed = 0
    now = datetime.datetime.now(datetime.UTC).isoformat() + "Z"

    for table in tables:
        try:
            cur = conn.cursor()

            # Determine column names
            if family == "mysql":
                cur.execute(f"SELECT * FROM `{table}` LIMIT 0")
            else:
                cur.execute(f'SELECT * FROM "{table}" LIMIT 0')

            columns = [d[0] for d in cur.description]
            cur.close()

            # Fetch rows in batches using LIMIT / OFFSET
            offset = 0
            table_rows_found = 0

            while True:
                cur = conn.cursor()

                if family == "mysql":
                    cur.execute(
                        f"SELECT * FROM `{table}` LIMIT %s OFFSET %s",
                        (BATCH_SIZE, offset),
                    )
                else:
                    cur.execute(
                        f'SELECT * FROM "{table}" LIMIT %s OFFSET %s',
                        (BATCH_SIZE, offset),
                    )

                raw_rows = cur.fetchall()
                cur.close()

                if not raw_rows:
                    break

                batch = []
                for raw in raw_rows:
                    record = _normalize_row(columns, raw)
                    batch.append(
                        {
                            "uid": uid,
                            "source": SOURCE,
                            "table_name": table,
                            "record_id": str(
                                record.get("id")
                                or record.get("ID")
                                or hash(str(record))
                            ),
                            "data_json": json.dumps(record, default=str),
                            "raw_json": json.dumps(record, default=str),
                            "fetched_at": now,
                        }
                    )

                table_rows_found += len(batch)

                if dest_cfg and batch:
                    pushed = push_to_destination(dest_cfg, SOURCE, batch)
                    total_rows_pushed += pushed

                offset += BATCH_SIZE

                if len(raw_rows) < BATCH_SIZE:
                    break

            total_rows_found += table_rows_found
            tables_processed += 1
            _log(f"table={table} rows={table_rows_found}")

        except Exception as e:
            _log(f"Failed extracting table '{table}': {e}")
            continue

    conn.close()

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


def disconnect_rds(uid: str) -> dict:
    """Disable the connector in google_connections and mark status disconnected."""
    _set_connection_enabled(uid, False)
    _update_status(uid, "disconnected")
    _log(f"Disconnected uid={uid}")
    return {"status": "disconnected"}

def _get_active_destination(uid: str) -> dict | None:

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, SOURCE))

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
        "database_name": row[5]
    }