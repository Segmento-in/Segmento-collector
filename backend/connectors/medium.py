import feedparser
import sqlite3
import os
import datetime
import json

DB = os.getenv("DB_PATH", "identity.db")


def db():
    return sqlite3.connect(DB, timeout=30)


def get_username(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
        SELECT username
        FROM medium_accounts
        WHERE uid=?
    """, (uid,))

    row = cur.fetchone()
    con.close()

    if not row:
        raise Exception("Medium not connected")

    return row[0]


def sync_medium(uid, sync_type="historical"):

    username = get_username(uid)

    url = f"https://medium.com/feed/@{username}"
    feed = feedparser.parse(url)

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    # Get last state
    cur.execute("""
        SELECT last_published
        FROM medium_state
        WHERE uid=?
    """, (uid,))
    row = cur.fetchone()

    last_published = row[0] if row else None

    inserted_rows = []
    newest_published = None

    for e in feed.entries[:50]:

        published = e.get("published")

        if sync_type == "incremental" and last_published:
            if published <= last_published:
                continue

        cur.execute("""
            INSERT OR IGNORE INTO medium_posts
            (uid, title, link, author, published,
             summary, raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            uid,
            e.get("title"),
            e.get("link"),
            e.get("author"),
            published,
            e.get("summary"),
            json.dumps(dict(e)),
            now
        ))

        if cur.rowcount > 0:
            inserted_rows.append({
                "title": e.get("title"),
                "link": e.get("link"),
                "author": e.get("author"),
                "published": published
            })

        if not newest_published or published > newest_published:
            newest_published = published

    # Save state
    if newest_published:
        cur.execute("""
            INSERT OR REPLACE INTO medium_state
            (uid, last_published)
            VALUES (?,?)
        """, (uid, newest_published))

    con.commit()
    con.close()

    return {
        "posts": len(inserted_rows),
        "rows": inserted_rows
    }