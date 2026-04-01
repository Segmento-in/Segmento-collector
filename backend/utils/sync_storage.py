import sqlite3
import json
import uuid
import datetime

DB = "identity.db"

def init_sync_db():
    try:
        con = sqlite3.connect(DB)
        cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS connector_sync_log (
                id TEXT PRIMARY KEY,
                uid TEXT,
                source TEXT,
                data TEXT,
                row_count INTEGER,
                created_at TIMESTAMP,
                expires_at TIMESTAMP
            )
        """)
        
        # Create indexes for faster lookup
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_log_uid_source ON connector_sync_log(uid, source)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_log_expires ON connector_sync_log(expires_at)")
        
        con.commit()
        con.close()
    except Exception as e:
        print(f"[SYNC STORAGE INIT ERROR] {e}", flush=True)

# Auto initialize the DB table on import
init_sync_db()

def store_sync_data(uid, source, rows):
    """
    Stores a batch of connector data natively for 24h recovery purposes
    """
    if not rows or not uid:
        return

    try:
        now = datetime.datetime.utcnow()
        expires = now + datetime.timedelta(hours=24)

        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        expires_str = expires.strftime("%Y-%m-%d %H:%M:%S")
        
        row_count = len(rows)
        data_json = json.dumps(rows)
        record_id = str(uuid.uuid4())
        
        con = sqlite3.connect(DB)
        cur = con.cursor()
        
        cur.execute("""
            INSERT INTO connector_sync_log 
            (id, uid, source, data, row_count, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            record_id,
            uid,
            source,
            data_json,
            row_count,
            now_str,
            expires_str
        ))
        
        con.commit()
        con.close()
        print(f"[SYNC STORAGE] Buffered {row_count} rows for '{source}' (uid={uid}) against 24h recovery", flush=True)
        
    except Exception as e:
        print(f"[SYNC STORAGE ERROR] {e}", flush=True)


def get_recent_sync_data(uid, source):
    """
    Retrieves all unexpired batches synchronously, returning them as a list of parsed JSON rows.
    """
    if not uid or not source:
        return []

    try:
        now = datetime.datetime.now(datetime.UTC).isoformat()
        
        con = sqlite3.connect(DB)
        cur = con.cursor()
        
        cur.execute("""
            SELECT data FROM connector_sync_log
            WHERE uid=? AND source=? 
            AND datetime(expires_at) > datetime('now')
            ORDER BY created_at ASC
        """, (uid, source))
        
        recent_data_batches = []
        for row in cur.fetchall():
            try:
                batch_rows = json.loads(row[0])
                if batch_rows:
                    recent_data_batches.append(batch_rows)
            except json.JSONDecodeError:
                continue
                
        con.close()
        return recent_data_batches
        
    except Exception as e:
        print(f"[RECOVERY READ ERROR] {e}", flush=True)
        return []


def cleanup_expired_data():
    """
    Removes historic rows exceeding their 24h limitation
    """
    try:
        print("[CLEANUP] Running cleanup job", flush=True)
        now = datetime.datetime.now(datetime.UTC).isoformat()
        
        con = sqlite3.connect(DB)
        cur = con.cursor()
        
        cur.execute("""
    DELETE FROM connector_sync_log 
    WHERE datetime(expires_at) <= datetime('now')
""")
        
        deleted = cur.rowcount
        con.commit()
        con.close()
        
        print(f"[CLEANUP] Deleted rows: {deleted}", flush=True)
        
        if deleted > 0:
            print(f"[SYNC STORAGE SCHEDULER] Pruned {deleted} expired connector sync logs", flush=True)
            
    except Exception as e:
        print(f"[SYNC STORAGE CLEANUP ERROR] {e}", flush=True)

def test_cleanup():
    from backend.utils.sync_storage import cleanup_expired_data
    cleanup_expired_data()

test_cleanup()
