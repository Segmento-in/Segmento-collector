import json
import time
import sqlite3
import datetime

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from destinations.destination_router import push_to_destination


DB = "identity.db"
SOURCE = "forms"


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

    # Get latest token for Forms
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

def list_forms(drive):

    res = drive.files().list(
        q="mimeType='application/vnd.google-apps.form'",
        fields="files(id,name,modifiedTime)"
    ).execute()

    return res.get("files", [])


def fetch_form_meta(form_id, service):

    return service.forms().get(
        formId=form_id
    ).execute()


def fetch_responses(form_id, service, since=None):

    rows = []
    token = None


    while True:

        res = service.forms().responses().list(
            formId=form_id,
            pageToken=token
        ).execute()


        for r in res.get("responses", []):

            updated = r.get("lastSubmittedTime")

            if since and updated and updated <= since:
                continue


            rows.append({
                "form_id": form_id,
                "response_id": r.get("responseId"),
                "email": r.get("respondentEmail"),
                "created": r.get("createTime"),
                "updated": updated,
                "answers": json.dumps(r.get("answers")),
                "raw_json": json.dumps(r)
            })


        token = res.get("nextPageToken")

        if not token:
            break

        time.sleep(0.2)


    return rows


# ---------------- MAIN ---------------- #

def sync_forms():

    print("[FORMS] Starting sync...")


    # -------- AUTH --------
    uid, creds = get_creds()

    if not uid:
        print("[FORMS] Not connected")
        return {
            "status": "error",
            "message": "Forms not connected"
        }

    print(f"[FORMS] Connected as {uid}")


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

    print(f"[FORMS] Sync type: {sync_type}")


    # -------- DEST --------
    dest_cfg = get_active_destination(uid)

    if not dest_cfg:
        print("[FORMS] No destination")
        return {
            "status": "error",
            "message": "No destination"
        }

    print(f"[FORMS] Destination: {dest_cfg['type']}")


    # -------- STATE --------
    state = get_state(uid)

    last = None

    if state and sync_type in ("delta", "incremental"):
        last = state.get("last_updated")
        print(f"[FORMS] Incremental from {last}")
    else:
        print("[FORMS] Full sync")


    # -------- API --------
    drive = build("drive", "v3", credentials=creds)
    forms = build("forms", "v1", credentials=creds)


    # -------- LIST FORMS --------
    forms_list = list_forms(drive)

    print(f"[FORMS] Found {len(forms_list)} forms")


    rows = []


    # -------- FETCH --------
    for f in forms_list:

        fid = f["id"]

        meta = fetch_form_meta(fid, forms)

        info = meta.get("info", {})


        rows.append({
            "form_id": fid,
            "title": info.get("title"),
            "description": info.get("description"),
            "is_quiz": meta.get("settings", {}).get("quizSettings") is not None,
            "raw_json": json.dumps(meta),
            "updated": f.get("modifiedTime")
        })


        responses = fetch_responses(fid, forms, last)

        rows.extend(responses)


    print(f"[FORMS] Total rows: {len(rows)}")


    if not rows:
        return {
            "status": "ok",
            "records": 0,
            "message": "No changes"
        }


    # -------- ROUTER --------
    print("[FORMS] Pushing...")

    push_to_destination(dest_cfg, SOURCE, rows)

    print(f"[FORMS] Pushed {len(rows)} rows")


    # -------- STATE --------
    newest = max(
        r["updated"]
        for r in rows
        if r.get("updated")
    )

    save_state(uid, {
        "last_updated": newest
    })

    print(f"[FORMS] State updated to {newest}")


    return {
        "status": "ok",
        "records": len(rows)
    }