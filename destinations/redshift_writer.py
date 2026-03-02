import json
from datetime import datetime

import psycopg2


def _safe_ident(value):
    cleaned = "".join(
        ch if (ch.isalnum() or ch == "_") else "_"
        for ch in (value or "")
    )
    return cleaned.strip("_") or "default"


def push_redshift(dest, source, rows):
    if not rows:
        return 0

    host = dest["host"]
    port = int(dest.get("port") or 5439)
    user = dest["username"]
    password = dest["password"]

    db_name_raw = (dest.get("database_name") or "dev").strip()
    if "." in db_name_raw:
        database, schema = db_name_raw.split(".", 1)
    else:
        database, schema = db_name_raw, "public"

    schema = _safe_ident(schema)
    table = f"{_safe_ident(source)}_data"
    full_table = f"{schema}.{table}"

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=database,
        connect_timeout=15,
        sslmode="prefer",
    )

    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {full_table} (
                    id BIGINT IDENTITY(1,1),
                    payload VARCHAR(65535),
                    fetched_at TIMESTAMP
                )
                """
            )

            now = datetime.utcnow()
            values = [
                (json.dumps(row), now)
                for row in rows
            ]

            cur.executemany(
                f"""
                INSERT INTO {full_table} (payload, fetched_at)
                VALUES (%s, %s)
                """,
                values,
            )

        conn.commit()
        print(f"[REDSHIFT] Inserted {len(rows)} rows into {full_table}")
        return len(rows)

    finally:
        conn.close()
