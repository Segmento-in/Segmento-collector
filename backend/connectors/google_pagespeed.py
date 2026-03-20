import requests
import sqlite3
import datetime
import json
import time

from backend.destinations.destination_router import push_to_destination


SOURCE = "pagespeed"
DB = "identity.db"

BASE_URL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"


# DB CONNECTION
def db_connect():
    return sqlite3.connect(DB)

# CONNECTED USER

def get_connected_user():

    con = db_connect()
    cur = con.cursor()

    # must be explicitly connected
    cur.execute("""
        SELECT uid
        FROM google_connections
        WHERE source=? AND enabled=1
        LIMIT 1
    """, (SOURCE,))

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return row[0]

# API KEY (FROM connector_configs)

def get_api_key(uid):

    con = db_connect()
    cur = con.cursor()

    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='pagespeed'
        LIMIT 1
    """, (uid,))

    row = cur.fetchone()
    con.close()

    return row[0] if row else None

# DESTINATION

def get_active_destination(uid):

    con = db_connect()
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

# API CALL

def fetch_pagespeed(url, strategy, categories, api_key):

    params = {
        "url": url,
        "strategy": strategy,
        "key": api_key
    }

    for c in categories:
        params.setdefault("category", []).append(c)

    for attempt in range(5):

        try:
            r = requests.get(BASE_URL, params=params, timeout=60)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"[PageSpeed] Rate limited → retry {wait}s", flush=True)
                time.sleep(wait)
                continue

            raise Exception(f"{r.status_code}: {r.text}")

        except Exception as e:

            print("[PageSpeed ERROR]", e, flush=True)

            if attempt == 4:
                raise e

            time.sleep(3)

# SCORE EXTRACTION

def extract_scores(data):

    cats = data.get("lighthouseResult", {}).get("categories", {})

    def score(name):
        v = cats.get(name, {}).get("score")
        return float(v) * 100 if v is not None else None

    return {
        "performance": score("performance"),
        "seo": score("seo"),
        "accessibility": score("accessibility"),
        "best-practices": score("best-practices"),
        "pwa": score("pwa")
    }

# MAIN SYNC

def sync_pagespeed(url, sync_type="incremental"):

    uid = get_connected_user()

    if not uid:
        return {
            "status": "error",
            "message": "PageSpeed connector not connected"
        }

    api_key = get_api_key(uid)

    if not api_key:
        return {
            "status": "error",
            "message": "API key not configured"
        }

    strategies = ["mobile", "desktop"]

    categories = [
        "performance",
        "seo",
        "accessibility",
        "best-practices",
        "pwa"
    ]

    con = db_connect()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    rows_for_destination = []
    count = 0

    for strategy in strategies:

        print(f"[PageSpeed] Fetching {url} ({strategy})", flush=True)

        data = fetch_pagespeed(
            url,
            strategy,
            categories,
            api_key
        )

        scores = extract_scores(data)

        # ---------------- SAVE LOCAL ----------------
        cur.execute("""
            INSERT INTO google_pagespeed
            (
                uid,
                url,
                strategy,
                categories,
                performance_score,
                seo_score,
                accessibility_score,
                best_practices_score,
                pwa_score,
                raw_response,
                fetched_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            url,
            strategy,
            ",".join(categories),
            scores["performance"],
            scores["seo"],
            scores["accessibility"],
            scores["best-practices"],
            scores["pwa"],
            json.dumps(data),
            now
        ))

        rows_for_destination.append({
            "url": url,
            "strategy": strategy,
            "performance_score": scores["performance"],
            "seo_score": scores["seo"],
            "accessibility_score": scores["accessibility"],
            "best_practices_score": scores["best-practices"],
            "pwa_score": scores["pwa"]
        })

        count += 1
        time.sleep(1)

    con.commit()
    con.close()

    # ---------------- DESTINATION PUSH ----------------
    dest = get_active_destination(uid)

    if dest and rows_for_destination:
        push_to_destination(dest, SOURCE, rows_for_destination)

    return {
        "status": "success",
        "count": count
    }