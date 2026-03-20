import json
import time
import sqlite3
import datetime

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from backend.destinations.destination_router import push_to_destination


# ---------------- CONFIG ---------------- #

DB = "identity.db"
SOURCE = "contacts"


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

    # Get latest token for Contacts
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

# ---------------- FETCH ---------------- #

def fetch_contacts(service):

    people = []
    page_token = None


    while True:

        res = service.people().connections().list(
            resourceName="people/me",
            pageSize=1000,
            pageToken=page_token,
            personFields="names,emailAddresses,phoneNumbers,organizations,addresses,metadata"
        ).execute()


        people.extend(res.get("connections", []))


        page_token = res.get("nextPageToken")

        if not page_token:
            break


        time.sleep(0.2)


    return people


# ---------------- MAIN ---------------- #

def sync_contacts():

    print("[CONTACTS] Starting sync...", flush=True)


    # -------- AUTH --------
    uid, creds = get_creds()

    if not uid:
        print("[CONTACTS] Not connected", flush=True)
        return {
            "status": "error",
            "message": "Contacts not connected"
        }


    print(f"[CONTACTS] Connected as {uid}", flush=True)


    # -------- JOB --------
    con = get_db()
    cur = con.cursor()

    cur.execute("""
        SELECT sync_type
        FROM connector_jobs
        WHERE uid=? AND source=? AND enabled=1
        LIMIT 1
    """, (uid, SOURCE))

    row = cur.fetchone()
    con.close()

    sync_type = row[0] if row else "delta"

    print(f"[CONTACTS] Sync type: {sync_type}", flush=True)


    # -------- DEST --------
    dest_cfg = get_active_destination(uid)

    if not dest_cfg:
        print("[CONTACTS] No destination", flush=True)
        return {
            "status": "error",
            "message": "No destination"
        }


    print(f"[CONTACTS] Destination: {dest_cfg['type']}", flush=True)


    # -------- STATE --------
    state = get_state(uid)

    if state and sync_type in ("delta", "incremental"):
        print(f"[CONTACTS] Incremental from {state.get('last_updated')}", flush=True)
    else:
        print("[CONTACTS] Full sync", flush=True)


    # -------- API --------
    service = build(
        "people",
        "v1",
        credentials=creds,
        cache_discovery=False
    )


    # -------- FETCH --------
    try:
        people = fetch_contacts(service)

    except HttpError as e:
        print("[CONTACTS] API Error:", e, flush=True)
        raise


    rows = []


    for p in people:

        rows.append({
            "resource_name": p.get("resourceName"),
            "etag": p.get("etag"),

            "names": json.dumps(p.get("names")),
            "emails": json.dumps(p.get("emailAddresses")),
            "phones": json.dumps(p.get("phoneNumbers")),

            "organizations": json.dumps(p.get("organizations")),
            "addresses": json.dumps(p.get("addresses")),

            "metadata": json.dumps(p.get("metadata")),
            "raw_json": json.dumps(p)
        })


    print(f"[CONTACTS] Found {len(rows)} contacts", flush=True)


    if not rows:
        return {
            "status": "ok",
            "count": 0
        }


    # -------- ROUTER --------
    print("[CONTACTS] Pushing...", flush=True)

    push_to_destination(dest_cfg, SOURCE, rows)

    print(f"[CONTACTS] Pushed {len(rows)} rows", flush=True)


    # -------- STATE SAVE --------
    save_state(uid, {
        "last_updated": datetime.datetime.utcnow().isoformat()
    })


    print("[CONTACTS] State updated", flush=True)


    return {
        "status": "ok",
        "count": len(rows)
    }