from destinations.mysql_writer import push_to_mysql
from destinations.postgres_writer import push_postgres
from destinations.bigquery_writer import push_bigquery
from destinations.snowflake_writer import push_snowflake
from destinations.clickhouse_writer import push_clickhouse
from destinations.s3_writer import push_s3
from destinations.azure_datalake_writer import push_azure_datalake
from destinations.databricks_writer import push_databricks
from destinations.redshift_writer import push_redshift
from security.secure_db import decrypt_payload
from flask import g, has_request_context

import sqlite3
import datetime

DB = "identity.db"

def resolve_destination_format(dest_cfg, source):

    dest_type = dest_cfg.get("type")

    # only needed for supported destinations
    if dest_type not in ["s3", "bigquery", "azure_datalake"]:
        return dest_cfg

    try:
        con = sqlite3.connect(DB)
        cur = con.cursor()

        cur.execute("""
            SELECT format
            FROM destination_configs
            WHERE source=?
            AND dest_type=?
            AND is_active=1
            ORDER BY id DESC
            LIMIT 1
        """, (source, dest_type))

        row = cur.fetchone()
        con.close()

        if row and row[0]:
            dest_cfg["format"] = row[0].lower()
            print("[ROUTER] FORMAT RESOLVED FROM DB:", row[0])

    except Exception as e:
        print("[ROUTER FORMAT ERROR]", e)

    return dest_cfg

def push_to_destination(dest_cfg, source, rows):

    if not rows:
        return 0

    # FORCE DECRYPT DESTINATION CONFIG
    dest_cfg = decrypt_payload(dict(dest_cfg))

    # CENTRAL FORMAT RESOLUTION
    dest_cfg = resolve_destination_format(dest_cfg, source)

    dest_type = dest_cfg["type"]

    # Extract uid for logging
    uid = None

    if has_request_context():
        uid = getattr(g, "user_id", None)

    # ---------------- FORMAT ISOLATION ----------------
    if dest_type in ["bigquery", "s3", "azure_datalake"]:
        dest_cfg["format"] = (
            dest_cfg.get("format") or "parquet"
        ).lower()
    else:
        dest_cfg.pop("format", None)

    try:

        if dest_type == "mysql":
            count = push_to_mysql(dest_cfg, source, rows)

        elif dest_type == "postgres":
            count = push_postgres(dest_cfg, source, rows)

        elif dest_type == "bigquery":
            count = push_bigquery(dest_cfg, source, rows)

        elif dest_type == "snowflake":
            count = push_snowflake(dest_cfg, source, rows)

        elif dest_type == "clickhouse":
            count = push_clickhouse(dest_cfg, source, rows)

        elif dest_type == "s3":
            count = push_s3(dest_cfg, source, rows)

        elif dest_type == "azure_datalake":
            count = push_azure_datalake(dest_cfg, source, rows)

        elif dest_type == "databricks":
            count = push_databricks(dest_cfg, source, rows)

        elif dest_type == "redshift":
            count = push_redshift(dest_cfg, source, rows)

        else:
            raise Exception(
                f"Unsupported destination: {dest_type}"
            )

        # SUCCESS LOG
        log_destination_push(
            uid,
            source,
            dest_type,
            count,
            "success"
        )

        return count

    except Exception as e:

        # FAILURE LOG
        log_destination_push(
            uid,
            source,
            dest_type,
            0,
            "failed",
            str(e)
        )

        raise e

# ---------------- USAGE DESTINATION LOGGER ----------------

def log_destination_push(uid, source, dest_type,
                         rows, status, error=None):

    con = sqlite3.connect(DB)
    cur = con.cursor()

    cur.execute("""
        INSERT INTO destination_push_logs
        (uid, source, destination_type,
         rows_pushed, pushed_at,
         status, error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        uid,
        source,
        dest_type,
        rows,
        datetime.datetime.now(
            datetime.UTC
        ).isoformat(),
        status,
        error
    ))

    con.commit()
    con.close()
