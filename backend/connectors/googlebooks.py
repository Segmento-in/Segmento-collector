import requests
import sqlite3
import datetime
import json
import os
from dotenv import load_dotenv
from backend.destinations.destination_router import push_to_destination

load_dotenv()
SOURCE = "books"
DB = os.getenv("DB_PATH", "identity.db")

API_KEY = os.getenv("GOOGLE_API_KEY")

BASE = "https://www.googleapis.com/books/v1/volumes"

# ---------------- DB ----------------

def db():
    return sqlite3.connect(
        DB,
        timeout=60,
        check_same_thread=False,
        isolation_level="DEFERRED" 
    )


# ---------------- API ----------------

def books_get(params):

    if not API_KEY:
        raise Exception("GOOGLE_BOOKS_API_KEY missing")

    params["key"] = API_KEY

    r = requests.get(BASE, params=params, timeout=20)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()


# ---------------- STATE ----------------

def get_state(uid, query):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT last_index FROM google_books_state
    WHERE uid=? AND query=?
    """, (uid, query))

    row = cur.fetchone()
    con.close()

    return row[0] if row else 0


# ---------------- PARSE ----------------

def parse_volume(v):

    info = v.get("volumeInfo", {})

    return {
        "id": v.get("id"),
        "title": info.get("title"),
        "authors": ", ".join(info.get("authors", [])),
        "publisher": info.get("publisher"),
        "published": info.get("publishedDate"),
        "description": info.get("description"),
        "pages": info.get("pageCount"),
        "categories": ", ".join(info.get("categories", [])),
        "language": info.get("language"),
        "preview": info.get("previewLink"),
        "raw": v
    }


# ---------------- SYNC VOLUMES ----------------

def sync_books(query, sync_type="incremental", limit=500):

    uid = get_connected_user()

    if not uid:
        return {
            "status": "error",
            "message": "Books connector not connected"
        }

    start = 0

    if sync_type == "incremental":
        start = get_state(uid, query)

    fetched = 0
    rows_for_destination = []

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    while fetched < limit:

        data = books_get({
            "q": f'intitle:{query} OR subject:{query}',
            "startIndex": start,
            "maxResults": 40,
            "langRestrict": "en"
        })

        items = data.get("items", [])
        if not items:
            break

        for v in items:

            p = parse_volume(v)

            cur.execute("""
                INSERT OR IGNORE INTO google_books_volumes
                (uid, volume_id, title, authors,
                 publisher, published_date,
                 description, page_count,
                 categories, language,
                 preview_link, raw_json,
                 fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                uid,
                p["id"],
                p["title"],
                p["authors"],
                p["publisher"],
                p["published"],
                p["description"],
                p["pages"],
                p["categories"],
                p["language"],
                p["preview"],
                json.dumps(p["raw"]),
                now
            ))

            # prepare row for destination
            rows_for_destination.append({
                "volume_id": p["id"],
                "title": p["title"],
                "authors": p["authors"],
                "publisher": p["publisher"],
                "published_date": p["published"],
                "description": p["description"],
                "page_count": p["pages"],
                "categories": p["categories"],
                "language": p["language"],
                "preview_link": p["preview"],
                "query": query
            })

            fetched += 1

        start += len(items)

        cur.execute("""
            INSERT OR REPLACE INTO google_books_state
            (uid, query, last_index)
            VALUES (?,?,?)
        """, (uid, query, start))

        con.commit()

        if len(items) < 40:
            break

    con.close()

    # ---------- Push to destination ----------
    dest = get_active_destination(uid)

    if dest and rows_for_destination:
        push_to_destination(dest, SOURCE, rows_for_destination)

    return {
        "status": "success",
        "fetched": fetched,
        "next_index": start
    }

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

def get_active_destination(uid):
    con = db()
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
