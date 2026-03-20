import json
import time
import sqlite3
import datetime

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from backend.destinations.destination_router import push_to_destination


DB = "identity.db"
SOURCE = "ga4"


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

# ---------------- AUTH ---------------- #

def get_creds():

    con = get_db()
    cur = con.cursor()

    # Get token
    cur.execute("""
        SELECT uid, access_token, refresh_token, scopes
        FROM google_accounts
        WHERE source=?
        ORDER BY id DESC
        LIMIT 1
    """, (SOURCE,))

    token_row = cur.fetchone()

    if not token_row:
        con.close()
        return None, None, None

    uid, access, refresh, scopes = token_row

    # Get connector config (credentials + property)
    cur.execute("""
        SELECT client_id, client_secret, config_json
        FROM connector_configs
        WHERE uid=? AND connector=?
        LIMIT 1
    """, (uid, SOURCE))

    cfg = cur.fetchone()
    con.close()

    if not cfg:
        return None, None, None

    client_id, client_secret, config_json = cfg

    config = json.loads(config_json) if config_json else {}

    property_id = config.get("property_id")

    creds = Credentials(
        token=access,
        refresh_token=refresh,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes.split(",")
    )

    return uid, creds, property_id

# ---------------- GA4 REPORT ---------------- #

def run_report(service, property_id, dimensions, metrics, start, end):

    body = {
        "dateRanges": [{
            "startDate": start,
            "endDate": end
        }],
        "dimensions": [{"name": d} for d in dimensions],
        "metrics": [{"name": m} for m in metrics]
    }


    for i in range(5):

        try:

            res = service.properties().runReport(
                property=f"properties/{property_id}",
                body=body
            ).execute()

            return res

        except Exception:

            if i == 4:
                raise

            time.sleep(5)



# ---------------- FETCH ---------------- #

def fetch_reports(service, property_id, since_date=None):

    rows = []


    end = datetime.date.today() - datetime.timedelta(days=1)

    if since_date:
        start = datetime.datetime.fromisoformat(since_date).date() - datetime.timedelta(days=2)
    else:
        start = end - datetime.timedelta(days=30)


    start = start.strftime("%Y-%m-%d")
    end = end.strftime("%Y-%m-%d")


    reports = [

        # Overview
        {
            "table": "ga4_overview",
            "dimensions": ["date"],
            "metrics": ["totalUsers", "sessions"]
        },

        # Devices
        {
            "table": "ga4_devices",
            "dimensions": ["date", "deviceCategory", "browser"],
            "metrics": ["totalUsers"]
        },

        # Traffic
        {
            "table": "ga4_traffic",
            "dimensions": ["date", "sessionSource", "sessionMedium"],
            "metrics": ["sessions"]
        },

        # Events
        {
            "table": "ga4_events",
            "dimensions": ["date", "eventName"],
            "metrics": ["eventCount"]
        }
    ]


    for rpt in reports:

        res = run_report(
            service,
            property_id,
            rpt["dimensions"],
            rpt["metrics"],
            start,
            end
        )


        for r in res.get("rows", []):

            row = {
                "table": rpt["table"],
                "property_id": property_id,
                "date_range": f"{start}:{end}",
                "raw": json.dumps(r)
            }

            # Flatten values
            # Normalize to fixed schema (max 3 dims, 2 metrics)

            dims = [d["value"] for d in r.get("dimensionValues", [])]
            mets = [m["value"] for m in r.get("metricValues", [])]

            # Always same keys for BigQuery
            row["dim_0"] = dims[0] if len(dims) > 0 else None
            row["dim_1"] = dims[1] if len(dims) > 1 else None
            row["dim_2"] = dims[2] if len(dims) > 2 else None

            row["met_0"] = mets[0] if len(mets) > 0 else None
            row["met_1"] = mets[1] if len(mets) > 1 else None



            rows.append(row)


    return rows, end


# ---------------- MAIN ---------------- #

def sync_ga4():

    print("[GA4] Starting sync...", flush=True)


    # -------- AUTH --------
    uid, creds, property_id = get_creds()

    if not creds:
        return {"status": "not_connected"}


    service = build("analyticsdata", "v1beta", credentials=creds)


    # -------- STATE --------
    state = get_state(uid)

    last_sync = state.get("last_sync") if state else None


    # -------- PROPERTY --------
    if not property_id:
        return {"status": "missing_property_id"}

    # -------- FETCH --------
    rows, new_cursor = fetch_reports(
        service,
        property_id,
        last_sync
    )


    if not rows:
        return {"status": "no_data"}


    # -------- DEST --------
    dest = get_active_destination(uid)

    if not dest:
        return {"status": "no_destination"}


    pushed = push_to_destination(dest, SOURCE, rows)


    # -------- SAVE STATE --------
    save_state(uid, {
        "last_sync": new_cursor
    })


    print("[GA4] Done:", pushed, flush=True)


    return {
        "status": "success",
        "rows": pushed
    }