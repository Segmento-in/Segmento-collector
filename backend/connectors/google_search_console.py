import sqlite3
import datetime
import time
import json

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from backend.destinations.destination_router import push_to_destination


DB = "identity.db"
SOURCE = "search-console"


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

    return json.loads(row[0]) if row else None


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
        SELECT dest_type, host, port, username, password, database_name
        FROM destination_configs
        WHERE uid=? AND source=? AND active=1
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

    # Get enabled connection
    cur.execute("""
        SELECT uid
        FROM google_connections
        WHERE source=? AND enabled=1
        LIMIT 1
    """, (SOURCE,))

    row = cur.fetchone()

    if not row:
        con.close()
        return None, None

    uid = row[0]

    # Get token
    cur.execute("""
        SELECT access_token, refresh_token, scopes
        FROM google_accounts
        WHERE uid=? AND source=?
        ORDER BY id DESC
        LIMIT 1
    """, (uid, SOURCE))

    token_row = cur.fetchone()

    if not token_row:
        con.close()
        return None, None

    access_token, refresh_token, scopes = token_row

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
        token=access_token,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes.split(",")
    )

    return uid, creds

# ---------------- FETCH DATA ---------------- #

def fetch_gsc_data(service, site_url, start, end):

    request = {
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate": end.strftime("%Y-%m-%d"),
        "dimensions": ["query", "page", "country", "device"],
        "rowLimit": 25000
    }

    for i in range(5):
        try:
            res = service.searchanalytics().query(
                siteUrl=site_url,
                body=request
            ).execute()
            break
        except Exception:
            if i == 4:
                raise
            time.sleep(5)

    rows = res.get("rows", [])

    formatted = []

    for r in rows:
        keys = r.get("keys", ["", "", "", ""])

        formatted.append({
            "site_url": site_url,
            "query": keys[0],
            "page": keys[1],
            "country": keys[2],
            "device": keys[3],
            "clicks": r.get("clicks", 0),
            "impressions": r.get("impressions", 0),
            "ctr": r.get("ctr", 0),
            "position": r.get("position", 0),
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "fetched_at": datetime.datetime.utcnow().isoformat()
        })

    return formatted


# ---------------- MAIN SYNC ---------------- #

def sync_search_console(site_url, sync_type="incremental"):

    print("[GSC] Sync started...", flush=True)

    uid, creds = get_creds()

    if not creds:
        return {"status": "error", "message": "Not connected"}

    service = build("searchconsole", "v1", credentials=creds)

    today = datetime.date.today() - datetime.timedelta(days=1)

    if sync_type == "historical":
        start = today - datetime.timedelta(days=30)

    else:
        state = get_state(uid)
        if state and state.get("last_sync"):
            start = datetime.datetime.fromisoformat(
                state["last_sync"]
            ).date()
        else:
            start = today - datetime.timedelta(days=7)

    end = today

    data_rows = fetch_gsc_data(service, site_url, start, end)

    if not data_rows:
        return {"status": "success", "count": 0}

    # Push to destination
    dest = get_active_destination(uid)

    if dest:
        pushed = push_to_destination(dest, SOURCE, data_rows)
    else:
        pushed = 0

    # Save incremental state
    save_state(uid, {
        "last_sync": end.isoformat()
    })

    print("[GSC] Sync complete:", pushed, flush=True)

    return {
        "status": "success",
        "count": pushed
    }