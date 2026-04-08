import requests
import sqlite3
import os
import time
from datetime import datetime


BASE = "https://hacker-news.firebaseio.com/v0"
DB = os.getenv("DB_PATH", "/tmp/identity.db")


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


# ---------------- HTTP Helper ----------------

def safe_get(url):

    try:

        r = requests.get(url, timeout=15)

        if r.status_code == 200:
            return r.json()

        if r.status_code == 429:
            time.sleep(5)

    except Exception:
        time.sleep(3)

    return None


# ---------------- State Management ----------------

def get_last_id(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT last_story_id
    FROM hackernews_state
    WHERE uid=?
    """, (uid,))

    row = cur.fetchone()

    con.close()

    if row and row[0]:
        return row[0]

    return 0


def save_last_id(uid, story_id):

    con = db()
    cur = con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO hackernews_state
    (uid, last_story_id)
    VALUES (?,?)
    """, (uid, story_id))

    con.close()


# ---------------- API Fetchers ----------------

def fetch_top_ids():

    return safe_get(f"{BASE}/topstories.json")


def fetch_story(story_id):

    return safe_get(f"{BASE}/item/{story_id}.json")


# ---------------- Batch Insert ----------------

def insert_batch(uid, items):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    rows = []

    for i in items:

        rows.append((
            uid,

            i.get("id"),

            i.get("title"),
            i.get("by"),
            i.get("url"),

            i.get("score"),
            i.get("descendants"),

            i.get("type"),
            i.get("time"),

            str(i),

            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO hackernews_stories (

        uid,

        story_id,

        title,
        author,
        url,

        score,
        descendants,

        type,
        time,

        raw_json,

        fetched_at
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, rows)

    con.close()


# ---------------- Main Sync ----------------
from backend.destinations.destination_router import push_to_destination


def sync_hackernews(uid, sync_type="incremental", limit=50):

    if sync_type == "historical":
        last_id = 0
    else:
        last_id = get_last_id(uid)

    ids = fetch_top_ids()

    if not ids:
        return {
            "status": "error",
            "msg": "Failed to fetch story IDs"
        }

    new_items = []

    for sid in ids:

        if sync_type == "incremental" and sid <= last_id:
            continue

        data = fetch_story(sid)

        if not data:
            continue

        if data.get("type") != "story":
            continue

        new_items.append(data)

        if len(new_items) >= limit:
            break

        time.sleep(0.3)

    if not new_items:
        return {
            "status": "ok",
            "rows": [],
            "sync_type": sync_type
        }

    insert_batch(uid, new_items)

    max_id = max(i.get("id", 0) for i in new_items)
    save_last_id(uid, max_id)

    rows = []

    for i in new_items:
        rows.append({
            "story_id": i.get("id"),
            "title": i.get("title"),
            "author": i.get("by"),
            "url": i.get("url"),
            "score": i.get("score"),
            "descendants": i.get("descendants"),
            "type": i.get("type"),
            "time": i.get("time")
        })

    return {
        "status": "ok",
        "rows": rows,
        "stories": len(rows),
        "sync_type": sync_type
    }