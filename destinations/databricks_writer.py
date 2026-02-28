import databricks.sql
import json
from datetime import datetime

def push_databricks(dest, source, rows):

    if not rows:
        return 0

    try:
        connection = databricks.sql.connect(
            server_hostname=dest["host"],
            http_path=dest.get("port"),
            access_token=dest["password"],
            catalog=dest.get("database_name", "hive_metastore").split(".")[0],
            schema=dest.get("database_name", "hive_metastore.default").split(".")[-1] if "." in dest.get("database_name", "") else "default"
        )

        cursor = connection.cursor()

        table = f"{source}_data"

        # Create table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BIGINT GENERATED ALWAYS AS IDENTITY,
                payload STRING,
                fetched_at TIMESTAMP
            )
        """)

        # Databricks SQL doesn't have a PARSE_JSON equivalent directly for inserts from string literals in the same way Snowflake does,
        # but we can insert the JSON as a STRING (payload column) and parse it on read, which is very common in Delta tables.
        insert_sql = f"""
            INSERT INTO {table} (payload, fetched_at)
            VALUES (%s, %s)
        """

        now = datetime.utcnow()

        values = [
            (json.dumps(r), now)
            for r in rows
        ]

        cursor.executemany(insert_sql, values)

        connection.commit()
        cursor.close()
        connection.close()

        print(f"[DEST] Batch pushed {len(rows)} rows to Databricks")

        return len(rows)

    except Exception as e:
        print("[DATABRICKS ERROR]", e)
        raise e
