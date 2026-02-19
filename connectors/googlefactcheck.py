import requests
import sqlite3
import datetime
import json
import os
from dotenv import load_dotenv
from destinations.destination_router import push_to_destination

load_dotenv()

SOURCE = "factcheck"
DB = "identity.db"

BASE = "https://factchecktools.googleapis.com/v1alpha1/claims:search"


# ---------------- DB ---------------- #

def db():
    return sqlite3.connect(DB, timeout=60, check_same_thread=False)


# ---------------- CONNECTION CHECK ---------------- #

def get_connected_user():
    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT uid
        FROM google_connections
        WHERE source=? AND enabled=1
        LIMIT 1
    """, (SOURCE,))

    row = cur.fetchone()
    con.close()

    return row[0] if row else None


# ---------------- DESTINATION ---------------- #

def get_active_destination(uid):
    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT dest_type, host, port, username, password, database_name
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

def get_api_key(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='factcheck'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return row[0] if row else None

# ---------------- API ---------------- #

def factcheck_get(uid, params):

    api_key = get_api_key(uid)

    if not api_key:
        raise Exception("FactCheck API key not configured")

    params["key"] = api_key

    r = requests.get(BASE, params=params, timeout=20)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()

# ---------------- STATE ---------------- #

def get_state(uid, query):
    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT next_page_token
        FROM google_factcheck_state
        WHERE uid=? AND query=?
    """, (uid, query))

    row = cur.fetchone()
    con.close()

    return row[0] if row else None


def save_state(cur, uid, query, token):
    cur.execute("""
        INSERT OR REPLACE INTO google_factcheck_state
        (uid, query, next_page_token)
        VALUES (?,?,?)
    """, (uid, query, token))


# ---------------- PARSE ---------------- #

def parse_claim(c):

    reviews = c.get("claimReview", [])

    rating = None
    publisher = None
    review_url = None

    if reviews:
        r = reviews[0]
        rating = r.get("textualRating")
        publisher = r.get("publisher", {}).get("name")
        review_url = r.get("url")

    return {
        "claim_id": c.get("claimId"),
        "text": c.get("text"),
        "claimant": c.get("claimant"),
        "claim_date": c.get("claimDate"),
        "rating": rating,
        "publisher": publisher,
        "review_url": review_url
    }


# ---------------- MAIN SYNC ---------------- #

def sync_factcheck(uid, query, sync_type="incremental", limit=200):

    uid = get_connected_user()

    if not uid:
        return {"status": "error", "message": "FactCheck not connected"}

    token = None if sync_type == "historical" else get_state(uid, query)

    fetched = 0
    rows_for_destination = []

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    try:

        while fetched < limit:

            params = {
                "query": query,
                "pageSize": 20
            }

            if token:
                params["pageToken"] = token

            data = factcheck_get(params)

            claims = data.get("claims", [])
            token = data.get("nextPageToken")

            if not claims:
                break

            for c in claims:

                p = parse_claim(c)

                cur.execute("""
                    INSERT OR IGNORE INTO google_factcheck_claims
                    (uid, claim_id, text, claimant,
                     claim_date, rating,
                     review_publisher, review_url,
                     raw_json, fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    uid,
                    p["claim_id"],
                    p["text"],
                    p["claimant"],
                    p["claim_date"],
                    p["rating"],
                    p["publisher"],
                    p["review_url"],
                    json.dumps(c),
                    now
                ))

                rows_for_destination.append({
                    "claim_id": p["claim_id"],
                    "text": p["text"],
                    "claimant": p["claimant"],
                    "claim_date": p["claim_date"],
                    "rating": p["rating"],
                    "publisher": p["publisher"],
                    "review_url": p["review_url"],
                    "query": query
                })

                fetched += 1

                if fetched >= limit:
                    break

            save_state(cur, uid, query, token)
            con.commit()

            if not token:
                break

        con.close()

    except Exception as e:
        con.rollback()
        con.close()
        return {"status": "error", "message": str(e)}

    # ---------------- PUSH TO DESTINATION ---------------- #

    dest = get_active_destination(uid)

    if dest and rows_for_destination:

        normalized = []

        for r in rows_for_destination:
            clean = {}

            for k, v in r.items():
                if isinstance(v, (dict, list)):
                    clean[k] = json.dumps(v)
                else:
                    clean[k] = str(v) if v is not None else None

            normalized.append(clean)

        push_to_destination(dest, SOURCE, normalized)

    return {
        "status": "ok",
        "query": query,
        "fetched": fetched,
        "next_page_token": token
    }