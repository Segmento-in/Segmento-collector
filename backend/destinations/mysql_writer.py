import mysql.connector
from datetime import datetime


def push_to_mysql(dest, source, rows):

    if not rows:
        return 0

    conn = mysql.connector.connect(
        host=dest["host"],
        port=int(dest["port"]),
        user=dest["username"],
        password=dest["password"],
        database=dest["database_name"]
    )

    cur = conn.cursor()

    # ---------- Build Table Name ----------
    table = f"{source}_data"

    # ---------- Determine Columns ----------
    columns = list(rows[0].keys())

    # Check if fetched_at already exists
    has_fetched_at = "fetched_at" in columns

    col_defs = []

    for c in columns:
        col_defs.append(f"`{c}` TEXT")

    if not has_fetched_at:
        col_defs.append("fetched_at DATETIME")

    col_sql = ", ".join(col_defs)

    # ---------- Create Table ----------
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {col_sql}
        )
    """

    cur.execute(create_sql)

    # ---------- Insert Rows ----------
    cols = list(columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join([f"`{c}`" for c in cols])

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if has_fetched_at:
        insert_sql = f"""
            INSERT INTO {table}
            ({col_names})
            VALUES ({placeholders})
        """
    else:
        insert_sql = f"""
            INSERT INTO {table}
            ({col_names}, fetched_at)
            VALUES ({placeholders}, %s)
        """

    count = 0

    for r in rows:
        try:

            values = [str(r.get(c)) for c in cols]

            if not has_fetched_at:
                values.append(now)

            cur.execute(insert_sql, values)

            count += 1

        except Exception as e:
            print("[MYSQL] Insert error:", e, flush=True)

    conn.commit()
    conn.close()

    print(f"[DEST] Pushed {count} rows to MySQL ({table})", flush=True)

    return count