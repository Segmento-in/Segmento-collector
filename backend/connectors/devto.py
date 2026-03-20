import requests
import sqlite3
import datetime
import json
import time


DB = "identity.db"

BASE = "https://dev.to/api"

HEADERS = {
    "User-Agent": "Segmento"
}


# ---------------- DB ----------------

def db():
    return sqlite3.connect(DB, timeout=60, check_same_thread=False)


# ---------------- API ----------------

def devto_get(path, params=None):

    url = BASE + path

    r = requests.get(
        url,
        headers=HEADERS,
        params=params,
        timeout=20
    )

    if r.status_code == 429:
        time.sleep(60)
        return None

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()


# ---------------- STATE ----------------

def get_state(uid, endpoint):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT last_page
    FROM devto_state
    WHERE uid=? AND endpoint=?
    """, (uid, endpoint))

    row = cur.fetchone()
    con.close()

    return row[0] if row else 1


def save_state(con, cur, uid, endpoint, page):

    cur.execute("""
    INSERT OR REPLACE INTO devto_state
    (uid, endpoint, last_page)
    VALUES (?,?,?)
    """, (uid, endpoint, page))


# ---------------- PARSE ----------------

def parse_article(a):

    return {
        "id": a.get("id"),
        "title": a.get("title"),
        "url": a.get("url"),
        "author": a.get("user", {}).get("username"),
        "published": a.get("published_at"),
        "tags": ",".join(a.get("tag_list", [])),
        "reactions": a.get("positive_reactions_count"),
        "comments": a.get("comments_count"),
        "raw": a
    }


def parse_user(u):

    return {
        "id": u.get("id"),
        "username": u.get("username"),
        "name": u.get("name"),
        "url": u.get("profile_image_90"),
        "followers": u.get("followers_count"),
        "raw": u
    }


# ---------------- SYNC ARTICLES ----------------

def sync_articles(uid, sync_type="historical", limit=200):

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    fetched = 0
    page = 1
    new_rows = []
    stop = False

    while fetched < limit and not stop:

        data = devto_get("/articles", {
            "page": page,
            "per_page": 20
        })

        if not data:
            break

        for a in data:

            p = parse_article(a)

            # Check if article already exists
            cur.execute("""
                SELECT 1 FROM devto_articles
                WHERE uid=? AND article_id=?
            """, (uid, p["id"]))

            exists = cur.fetchone()

            if sync_type == "incremental" and exists:
                stop = True
                break

            cur.execute("""
                INSERT OR IGNORE INTO devto_articles
                (uid, article_id, title, url,
                 author, published_at, tags,
                 reactions, comments,
                 raw_json, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                uid,
                p["id"],
                p["title"],
                p["url"],
                p["author"],
                p["published"],
                p["tags"],
                p["reactions"],
                p["comments"],
                json.dumps(p["raw"]),
                now
            ))

            if cur.rowcount > 0:
                new_rows.append({
                    "article_id": p["id"],
                    "title": p["title"],
                    "url": p["url"],
                    "author": p["author"],
                    "published_at": p["published"],
                    "tags": p["tags"],
                    "reactions": p["reactions"],
                    "comments": p["comments"]
                })

            fetched += 1

        con.commit()
        page += 1

        if len(data) < 20:
            break

        time.sleep(1)

    con.close()

    print(f"[DEVTO] Sync type: {sync_type}", flush=True)
    print(f"[DEVTO] New rows found: {len(new_rows)}", flush=True)

    return {
        "articles": len(new_rows),
        "rows": new_rows
    }

# ---------------- SYNC TAGS ----------------

def sync_tags(uid):

    data = devto_get("/tags")

    if not data:
        return {"tags": 0, "rows": []}

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()
    new_rows = []

    for t in data:

        cur.execute("""
        INSERT OR IGNORE INTO devto_tags
        (uid, name, popularity,
         raw_json, fetched_at)
        VALUES (?,?,?,?,?)
        """, (
            uid,
            t.get("name"),
            t.get("popularity_score"),
            json.dumps(t),
            now
        ))

        if cur.rowcount > 0:
            new_rows.append({
                "name": t.get("name"),
                "popularity": t.get("popularity_score")
            })

    con.commit()
    con.close()

    print(f"[DEVTO] New tags: {len(new_rows)}", flush=True)

    return {
        "tags": len(new_rows),
        "rows": new_rows
    }
