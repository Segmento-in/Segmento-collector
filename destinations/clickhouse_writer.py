import clickhouse_connect
import json
from datetime import datetime

def push_clickhouse(dest, source, rows):

    if not rows:
        return 0

    try:
        client = clickhouse_connect.get_client(
            host=dest["host"],
            port=int(dest["port"]),
            username=dest["username"],
            password=dest["password"],
            database=dest["database_name"],
            secure=True
        )

        table = f"{source}_data"

        # Create table
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id UInt64,
                payload String,
                fetched_at DateTime
            )
            ENGINE = MergeTree()
            ORDER BY id
        """)

        now = datetime.utcnow()

        data = []
        for i, r in enumerate(rows):
            data.append([
                i,
                json.dumps(r),
                now
            ])

        client.insert(
            table,
            data,
            column_names=["id", "payload", "fetched_at"]
        )

        print(f"[DEST] Pushed {len(rows)} rows to ClickHouse")

        return len(rows)

    except Exception as e:
        print("[CLICKHOUSE ERROR]", e)
        raise e