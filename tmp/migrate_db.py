import sqlite3
import os

DB_PATH = "identity.db"

def migrate():
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        # Check if column exists
        cursor = conn.execute("PRAGMA table_info(connector_configs)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "config_json" not in columns:
            print("Adding config_json column to connector_configs...")
            conn.execute("ALTER TABLE connector_configs ADD COLUMN config_json TEXT;")
            conn.commit()
            print("Migration successful.")
        else:
            print("Column config_json already exists. Skipping.")
    except Exception as e:
        print(f"Migration error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
