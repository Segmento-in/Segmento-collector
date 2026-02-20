import requests
import sqlite3
import datetime
import json

DB = "identity.db"
BASE = "https://api.tumblr.com/v2/blog"


# ---------------- DB ----------------

def db():
    return sqlite3.connect(DB, timeout=30)


# ---------------- GET USER API KEY ----------------

def get_api_key(uid):

    con=db()
    cur=con.cursor()

    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='tumblr'
    """,(uid,))

    row=cur.fetchone()
    con.close()

    if not row:
        raise Exception("Tumblr not configured")

    return row[0]

# ---------------- API ----------------

def tumblr_get(uid, url, params=None):

    api_key = get_api_key(uid)

    if not params:
        params = {}

    params["api_key"] = api_key

    r = requests.get(url, params=params, timeout=20)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()["response"]


# ---------------- SYNC POSTS ----------------

def sync_posts(uid, blog_name, sync_type="historical"):

    con = db()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    # Get last state
    cur.execute("""
        SELECT last_post_id
        FROM tumblr_state
        WHERE uid=? AND blog_name=?
    """, (uid, blog_name))

    row = cur.fetchone()
    last_post_id = row[0] if row else None

    url = f"{BASE}/{blog_name}/posts"

    data = tumblr_get(uid, url, {"limit": 50})
    posts = data.get("posts", [])

    inserted_rows = []
    max_post_id = None

    for p in posts:

        post_id = p["id"]

        if sync_type == "incremental" and last_post_id:
            if post_id <= last_post_id:
                continue

        body = ""
        if p["type"] == "text":
            body = p.get("body", "")

        cur.execute("""
        INSERT OR IGNORE INTO tumblr_posts
        (uid, blog_name, post_id,
         post_type, title, body,
         url, timestamp,
         raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            blog_name,
            post_id,
            p["type"],
            p.get("title"),
            body,
            p.get("post_url"),
            p.get("timestamp"),
            json.dumps(p),
            now
        ))

        if cur.rowcount > 0:
            inserted_rows.append({
                "blog_name": blog_name,
                "post_id": post_id,
                "title": p.get("title"),
                "url": p.get("post_url"),
                "timestamp": p.get("timestamp")
            })

        if not max_post_id or post_id > max_post_id:
            max_post_id = post_id

    # Save state
    if max_post_id:
        cur.execute("""
            INSERT OR REPLACE INTO tumblr_state
            (uid, blog_name, last_post_id)
            VALUES (?,?,?)
        """, (uid, blog_name, max_post_id))

    con.commit()
    con.close()

    return {
        "posts": len(inserted_rows),
        "rows": inserted_rows
    }