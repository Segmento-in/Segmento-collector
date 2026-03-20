import json
import time
import sqlite3
import datetime

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from backend.destinations.destination_router import push_to_destination


DB = "identity.db"
SOURCE = "drive"


# ---------------- DB ---------------- #

def get_db():

    con = sqlite3.connect(
        DB,
        timeout=60,
        isolation_level=None,
        check_same_thread=False
    )

    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=60000;")
    con.execute("PRAGMA synchronous=NORMAL;")

    return con


# ---------------- STATE ---------------- #

def get_state(uid):

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT state_json
        FROM connector_state
        WHERE uid=? AND source=?
    """, (uid, SOURCE))

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return json.loads(row[0])


def save_state(uid, state):

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        INSERT OR REPLACE INTO connector_state
        (uid, source, state_json, updated_at)
        VALUES (?, ?, ?, ?)
    """, (
        uid,
        SOURCE,
        json.dumps(state),
        datetime.datetime.utcnow().isoformat()
    ))

    con.commit()
    con.close()


# ---------------- DESTINATION ---------------- #

def get_active_destination(uid):

    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT
            dest_type,
            host,
            port,
            username,
            password,
            database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND is_active=1
        LIMIT 1
    """, (uid, SOURCE))

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return {
        "type": row[0],
        "host": row[1],
        "port": row[2],
        "username": row[3],
        "password": row[4],
        "database_name": row[5]
    }


# ---------------- AUTH ---------------- #

# ---------------- AUTH ---------------- #

def get_creds():

    con = get_db()
    cur = con.cursor()

    # Fetch latest token for Drive
    cur.execute("""
        SELECT uid, access_token, refresh_token, scopes
        FROM google_accounts
        WHERE source=?
        ORDER BY id DESC
        LIMIT 1
    """, (SOURCE,))

    row = cur.fetchone()

    if not row:
        con.close()
        return None, None

    uid, access, refresh, scopes = row

    # Fetch Google App credentials from connector_configs
    cur.execute("""
        SELECT client_id, client_secret
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, SOURCE))

    cfg = cur.fetchone()
    con.close()

    if not cfg:
        return None, None

    client_id, client_secret = cfg

    creds = Credentials(
        token=access,
        refresh_token=refresh,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes.split(",")
    )

    return uid, creds

# ---------------- FETCH ---------------- #

def fetch_files(service, modified_after=None):

    rows = []
    page_token = None


    # Build query for incremental sync
    q = None

    if modified_after:
        q = f"modifiedTime > '{modified_after}'"


    while True:

        res = service.files().list(
            pageSize=200,
            pageToken=page_token,
            q=q,
            fields="nextPageToken, files(id,name,mimeType,size,createdTime,modifiedTime,webViewLink,owners(emailAddress))"
        ).execute()


        files = res.get("files", [])


        for f in files:

            rows.append({
                "file_id": f.get("id"),
                "name": f.get("name"),
                "mime_type": f.get("mimeType"),
                "size": f.get("size"),
                "created_time": f.get("createdTime"),
                "modified_time": f.get("modifiedTime"),
                "web_view_link": f.get("webViewLink"),
                "owner_email": (
                    f.get("owners", [{}])[0].get("emailAddress")
                    if f.get("owners") else None
                )
            })


        page_token = res.get("nextPageToken")

        if not page_token:
            break


        time.sleep(0.2)


    return rows


# ---------------- MAIN SYNC ---------------- #

def sync_drive_files():

    print("[DRIVE] Starting sync...", flush=True)


    # ---------------- AUTH ----------------
    uid, creds = get_creds()

    if not uid:
        print("[DRIVE] Not connected", flush=True)
        return {
            "status": "error",
            "message": "Drive not connected"
        }

    print(f"[DRIVE] Connected as {uid}", flush=True)


    # ---------------- JOB ----------------
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source=? AND enabled=1
        LIMIT 1
    """, (uid, SOURCE))

    job = cur.fetchone()
    con.close()

    sync_type = job[0] if job else "delta"

    print(f"[DRIVE] Sync type: {sync_type}", flush=True)


    # ---------------- DEST ----------------
    dest_cfg = get_active_destination(uid)

    if not dest_cfg:
        print("[DRIVE] No destination configured", flush=True)
        return {
            "status": "error",
            "message": "No active destination"
        }

    print(f"[DRIVE] Destination: {dest_cfg['type']}", flush=True)


    # ---------------- STATE ----------------
    state = get_state(uid)

    last_modified = None

    if state and sync_type in ("delta", "incremental"):
        last_modified = state.get("last_modified_time")
        print(f"[DRIVE] Incremental from {last_modified}", flush=True)
    else:
        print("[DRIVE] Full sync mode", flush=True)


    # ---------------- API ----------------
    service = build("drive", "v3", credentials=creds)


    # ---------------- FETCH ----------------
    rows = fetch_files(service, last_modified)

    print(f"[DRIVE] Found {len(rows)} files", flush=True)


    if not rows:
        return {
            "status": "ok",
            "files": 0,
            "message": "No new data"
        }


    # ---------------- ROUTER ----------------
    print("[DRIVE] Pushing to destination...", flush=True)

    push_to_destination(dest_cfg, SOURCE, rows)

    print(f"[DRIVE] Pushed {len(rows)} rows", flush=True)


    # ---------------- STATE SAVE ----------------
    newest = max(
        r["modified_time"]
        for r in rows
        if r.get("modified_time")
    )

    save_state(uid, {
        "last_modified_time": newest
    })

    print(f"[DRIVE] State updated to {newest}", flush=True)


    return {
        "status": "ok",
        "files": len(rows)
    }