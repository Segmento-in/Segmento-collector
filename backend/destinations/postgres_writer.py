import psycopg2
import json

def push_postgres(dest, source, rows):

    conn = psycopg2.connect(
        host=dest["host"],
        port=dest["port"],
        user=dest["username"],
        password=dest["password"],
        dbname=dest["database_name"]
    )

    cur = conn.cursor()

    table = f"{source}_data"


    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id SERIAL PRIMARY KEY,
            payload JSONB
        )
    """)


    count = 0

    for r in rows:

        cur.execute(
            f"INSERT INTO {table} (payload) VALUES (%s)",
            (json.dumps(r),)
        )

        count += 1


    conn.commit()
    cur.close()
    conn.close()

    print(f"[DEST] Pushed {count} rows to Postgres", flush=True)

    return count