import duckdb
import pandas as pd
import os
import tempfile
from datetime import datetime


def push_duckdb(dest, source, rows):
    """
    Push rows to DuckDB.
    Supports parquet and json.
    Mapping:
    - file_path -> host
    """
    if not rows:
        return 0

    db_path = dest["host"]
    fmt = (dest.get("format") or "parquet").lower()
    table_name = f"{source}_data"

    # Ensure directory exists for the db path
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = duckdb.connect(db_path)

    df = pd.DataFrame(rows)
    df["fetched_at"] = pd.Timestamp.utcnow()

    # We use a temporary file to leverage DuckDB's efficient file loading
    # although for small/medium datasets we could also just register the DF.
    # To keep it consistent with the "format" requirement:
    
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{fmt}")
    file_path = tmp.name
    tmp.close()

    try:
        if fmt == "parquet":
            df.to_parquet(file_path, index=False)
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet(?)", [file_path])
            # If table already exists, append
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet(?)", [file_path])
        elif fmt == "json":
            df.to_json(file_path, orient="records", lines=True)
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_json_auto(?)", [file_path])
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_json_auto(?)", [file_path])
        else:
            raise Exception(f"Unsupported DuckDB format: {fmt}")

        # Note: CREATE TABLE AS ... might fail if table exists, so we handle it by catching or checking.
        # Actually in DuckDB 0.9.x+ one can use:
        # conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM ... LIMIT 0")
        # But let's be safer:
        
        # Check if table exists
        exists = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table_name]).fetchone()[0]
        
        if not exists:
             if fmt == "parquet":
                 conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet(?)", [file_path])
             else:
                 conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto(?)", [file_path])
        else:
             if fmt == "parquet":
                 conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet(?)", [file_path])
             else:
                 conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_json_auto(?)", [file_path])

        count = len(rows)
        print(f"[DEST] Pushed {count} rows to DuckDB ({db_path}, table: {table_name})")

    finally:
        if os.path.exists(file_path):
            os.unlink(file_path)
        conn.close()

    return count
