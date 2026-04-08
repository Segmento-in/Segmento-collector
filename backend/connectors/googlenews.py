import feedparser
import sqlite3
import os
import datetime
import json
import urllib.parse

from backend.destinations.destination_router import push_to_destination

DB = os.getenv("DB_PATH", "/tmp/identity.db")
SOURCE = "news"


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


# ---------------- CONNECTION CHECK ---------------- #

def get_connected_user():
    con = get_db()
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


# ---------------- PARSE ---------------- #

def parse_entry(e):
    return {
        "article_id": e.get("id") or e.get("link"),
        "title": e.get("title"),
        "link": e.get("link"),
        "source_name": e.get("source", {}).get("title"),
        "published": e.get("published"),
        "summary": e.get("summary"),
        "raw_json": json.dumps(e)
    }


# ---------------- FETCH ---------------- #

def fetch_news(keyword, last_published=None, limit=200):

    encoded = urllib.parse.quote(keyword)
    url = (
        "https://news.google.com/rss/search"
        f"?q={encoded}&hl=en-IN&gl=IN&ceid=IN:en"
    )

    feed = feedparser.parse(url)

    rows = []
    newest = last_published

    for e in feed.entries:

        pub = e.get("published")

        if last_published and pub and pub <= last_published:
            continue

        row = parse_entry(e)
        row["keyword"] = keyword

        rows.append(row)

        if not newest or (pub and pub > newest):
            newest = pub

        if len(rows) >= limit:
            break

    return rows, newest


# ---------------- MAIN SYNC ---------------- #

def sync_news(keyword, sync_type="incremental"):

    uid = get_connected_user()

    if not uid:
        return {
            "status": "error",
            "message": "News connector not connected"
        }

    state = get_state(uid)

    last_published = None

    if sync_type == "incremental" and state:
        last_published = state.get("last_published")

    rows, newest = fetch_news(keyword, last_published)

    # Store locally (for dashboard)
    con = get_db()
    cur = con.cursor()

    for r in rows:
        cur.execute("""
            INSERT OR IGNORE INTO google_news_articles
            (uid, article_id, query ,
             title, link, source,
             published, summary,
             raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            r["article_id"],
            r["keyword"],
            r["title"],
            r["link"],
            r["source_name"],
            r["published"],
            r["summary"],
            r["raw_json"],
            datetime.datetime.utcnow().isoformat()
        ))

    con.commit()
    con.close()

    # Push to destination
    dest = get_active_destination(uid)

    if dest and rows:
        push_to_destination(dest, SOURCE, rows)

    # Save state
    if newest:
        save_state(uid, {
            "last_published": newest
        })

    return {
        "status": "success",
        "fetched": len(rows),
        "last_published": newest
    }