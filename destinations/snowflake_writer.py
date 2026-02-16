import snowflake.connector
import json
from datetime import datetime

def push_snowflake(dest, source, rows):

    if not rows:
        return 0

    try:
        conn = snowflake.connector.connect(
            user=dest["username"],
            password=dest["password"],
            account=dest["host"],
            warehouse=dest.get("port") or "COMPUTE_WH",
            database=dest["database_name"],
            schema="PUBLIC"
        )

        cur = conn.cursor()

        table = f"{source}_data"

        # Create table if not exists
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER AUTOINCREMENT,
                payload VARIANT,
                fetched_at TIMESTAMP_NTZ
            )
        """)

        insert_sql = f"""
            INSERT INTO {table}(payload, fetched_at)
            SELECT PARSE_JSON(%s), %s
        """

        now = datetime.utcnow()

        values = [
            (json.dumps(r), now)
            for r in rows
        ]

        cur.executemany(insert_sql, values)

        conn.commit()
        cur.close()
        conn.close()

        print(f"[DEST] Batch pushed {len(rows)} rows to Snowflake")

        return len(rows)

    except Exception as e:
        print("[SNOWFLAKE ERROR]", e)
        raise e