import requests
import sqlite3
import os
import time
from datetime import datetime


BASE = "https://en.wikipedia.org/w/api.php"
DB = os.getenv("DB_PATH", "identity.db")


HEADERS = {
    "User-Agent": "SegmentoCollector/1.0 (contact@demo.com)"
}


def safe_list(obj, key):

    """
    Safely extract list from API response
    Works even if obj[key] is missing or malformed
    """

    if not isinstance(obj, dict):
        return []

    val = obj.get(key)

    if isinstance(val, list):
        return val

    return []

# ---------------- DB Helper ----------------

def db():

    con = sqlite3.connect(
        DB,
        timeout=90,
        check_same_thread=False,
        isolation_level=None
    )

    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    return con


# ---------------- HTTP ----------------

def safe_get(params):

    try:

        r = requests.get(
            BASE,
            params=params,
            headers=HEADERS,
            timeout=15
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code == 429:
            time.sleep(60)

    except Exception:
        time.sleep(5)

    return None


# ---------------- State ----------------

def get_last_ts(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT last_rc_timestamp
    FROM wikipedia_state
    WHERE uid=?
    """, (uid,))

    row = cur.fetchone()

    con.close()

    if row and row[0]:
        return row[0]

    return "1970-01-01T00:00:00Z"


def save_last_ts(uid, ts):

    con = db()
    cur = con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO wikipedia_state
    (uid, last_rc_timestamp)
    VALUES (?,?)
    """, (uid, ts))

    con.close()


# ---------------- Inserts ----------------

def insert_recent(uid, rows):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    data = []


    for r in rows:

        data.append((
            uid,

            r.get("rcid"),

            r.get("title"),
            r.get("user"),
            r.get("comment"),

            r.get("timestamp"),

            str(r),

            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO wikipedia_recent_changes (

        uid,

        rcid,

        title,
        user,
        comment,

        timestamp,

        raw_json,
        fetched_at
    )
    VALUES (?,?,?,?,?,?,?,?)
    """, data)

    con.close()


def insert_new_pages(uid, rows):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    data = []


    for r in rows:

        data.append((
            uid,

            r.get("pageid"),

            r.get("title"),
            r.get("user"),

            r.get("timestamp"),

            str(r),

            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO wikipedia_new_pages (

        uid,

        page_id,

        title,
        creator,

        created_at,

        raw_json,
        fetched_at
    )
    VALUES (?,?,?,?,?,?,?)
    """, data)

    con.close()


def insert_views(uid, rows):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    data = []


    for i, r in enumerate(rows, start=1):

        data.append((
            uid,

            r.get("article"),

            r.get("views"),
            i,

            str(r),

            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO wikipedia_most_viewed (

        uid,

        article,

        views,
        rank,

        raw_json,
        fetched_at
    )
    VALUES (?,?,?,?,?,?)
    """, data)

    con.close()


# ---------------- Main Sync ----------------
def sync_wikipedia(uid, sync_type="incremental", rc_limit=50):

    last_ts = get_last_ts(uid)

    rows_for_destination = []
    new_changes = []
    new_pages_count = 0
    most_viewed_count = 0

    # -------- RECENT CHANGES --------
    rc = safe_get({
        "action": "query",
        "list": "recentchanges",
        "rcprop": "title|user|timestamp|comment|ids",
        "rclimit": rc_limit,
        "format": "json"
    })

    if rc:
        query = rc.get("query", {})
        changes = safe_list(query, "recentchanges")

        for c in changes:

            ts = c.get("timestamp")

            if sync_type == "incremental" and ts <= last_ts:
                continue

            new_changes.append(c)

            rows_for_destination.append({
                "uid": uid,
                "type": "recent_change",
                "rcid": c.get("rcid"),
                "title": c.get("title"),
                "user": c.get("user"),
                "comment": c.get("comment"),
                "timestamp": ts
            })

        if new_changes:
            insert_recent(uid, new_changes)
            latest = max(c["timestamp"] for c in new_changes)
            save_last_ts(uid, latest)

    # -------- NEW PAGES --------
    newp = safe_get({
        "action": "query",
        "list": "newpages",
        "nplimit": 30,
        "format": "json"
    })

    if newp:
        query = newp.get("query", {})
        pages = safe_list(query, "newpages")

        if pages:
            insert_new_pages(uid, pages)
            new_pages_count = len(pages)

            for p in pages:
                rows_for_destination.append({
                    "uid": uid,
                    "type": "new_page",
                    "page_id": p.get("pageid"),
                    "title": p.get("title"),
                    "creator": p.get("user"),
                    "created_at": p.get("timestamp")
                })

    # -------- MOST VIEWED --------
    views = safe_get({
        "action": "query",
        "list": "mostviewed",
        "pvimlimit": 20,
        "format": "json"
    })

    if views:
        query = views.get("query", {})
        most = query.get("mostviewed", {})
        items = safe_list(most, "articles")

        if items:
            insert_views(uid, items)
            most_viewed_count = len(items)

            for i, v in enumerate(items, start=1):
                rows_for_destination.append({
                    "uid": uid,
                    "type": "most_viewed",
                    "article": v.get("article"),
                    "views": v.get("views"),
                    "rank": i
                })

    return {
        "rows": rows_for_destination,
        "recent_changes": len(new_changes),
        "new_pages": new_pages_count,
        "most_viewed": most_viewed_count
    }