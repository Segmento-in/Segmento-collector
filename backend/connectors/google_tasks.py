import json
import time
import sqlite3
import datetime

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from backend.destinations.destination_router import push_to_destination

DB = "identity.db"
SOURCE = "tasks"

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


# ---------------- DEST ---------------- #

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
        WHERE uid=? AND source=?
        ORDER BY id DESC
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

def get_creds():

    con = get_db()
    cur = con.cursor()

    # Get latest token for Tasks
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

    # Get client credentials from connector_configs
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

# ---------------- HELPERS ---------------- #

def fetch_all(method, **kwargs):

    results = []
    token = None

    while True:

        res = method(pageToken=token, **kwargs).execute()

        results.extend(res.get("items", []))

        token = res.get("nextPageToken")

        if not token:
            break

        time.sleep(0.2)

    return results


# ---------------- FETCH ---------------- #

def fetch_tasks(service, updated_after=None):

    rows = []


    # Get all task lists
    lists = fetch_all(service.tasklists().list)

    for lst in lists:

        list_id = lst.get("id")
        list_title = lst.get("title")


        tasks = fetch_all(
            service.tasks().list,
            tasklist=list_id,
            showCompleted=True,
            showHidden=True
        )


        for t in tasks:

            updated = t.get("updated")

            if updated_after and updated and updated <= updated_after:
                continue


            rows.append({
                "list_id": list_id,
                "list_title": list_title,

                "task_id": t.get("id"),
                "title": t.get("title"),
                "status": t.get("status"),
                "due": t.get("due"),
                "completed": t.get("completed"),

                "updated": updated,
                "created": t.get("created"),

                "raw_json": json.dumps(t)
            })


    return rows


# ---------------- MAIN ---------------- #

def sync_tasks():

    print("[TASKS] Starting sync...", flush=True)


    # -------- AUTH --------
    uid, creds = get_creds()

    if not uid:
        print("[TASKS] Not connected", flush=True)
        return {
            "status": "error",
            "message": "Tasks not connected"
        }

    print(f"[TASKS] Connected as {uid}", flush=True)


    # -------- JOB --------
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

    print(f"[TASKS] Sync type: {sync_type}", flush=True)


    # -------- DEST --------
    dest_cfg = get_active_destination(uid)

    if not dest_cfg:
        print("[TASKS] No destination", flush=True)
        return {
            "status": "error",
            "message": "No destination"
        }

    print(f"[TASKS] Destination: {dest_cfg['type']}", flush=True)


    # -------- STATE --------
    state = get_state(uid)

    updated_after = None

    if state and sync_type in ("delta", "incremental"):
        updated_after = state.get("last_updated")
        print(f"[TASKS] Incremental from {updated_after}", flush=True)
    else:
        print("[TASKS] Full sync", flush=True)


    # -------- API --------
    service = build("tasks", "v1", credentials=creds)


    # -------- FETCH --------
    rows = fetch_tasks(service, updated_after)

    print(f"[TASKS] Found {len(rows)} tasks", flush=True)


    if not rows:
        return {
            "status": "ok",
            "tasks": 0,
            "message": "No new data"
        }


    # -------- ROUTER --------
    print("[TASKS] Pushing...", flush=True)

    push_to_destination(dest_cfg, SOURCE, rows)

    print(f"[TASKS] Pushed {len(rows)} rows", flush=True)


    # -------- STATE SAVE --------
    newest = max(
        r["updated"]
        for r in rows
        if r.get("updated")
    )

    save_state(uid, {
        "last_updated": newest
    })

    print(f"[TASKS] State updated to {newest}", flush=True)


    return {
        "status": "ok",
        "tasks": len(rows)
    }