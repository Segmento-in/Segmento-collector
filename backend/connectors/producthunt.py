from flask import json
import requests
import sqlite3
import time
import os
from datetime import datetime

DB = os.getenv("DB_PATH", "identity.db")

API_URL = "https://api.producthunt.com/v2/api/graphql"

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

def get_token(uid):

    con=db()
    cur=con.cursor()

    cur.execute("""
    SELECT config_json
    FROM connector_configs
    WHERE uid=? AND connector='producthunt'
    """,(uid,))

    row=cur.fetchone()
    con.close()

    if not row:
        raise Exception("ProductHunt token missing")

    return json.loads(row[0])["api_token"]

# ---------------- Token Helper ----------------

def get_headers(uid):

    token=get_token(uid)

    return {
        "Authorization":f"Bearer {token}",
        "Content-Type":"application/json"
    }

# ---------------- State ----------------

def get_last_time(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT last_post_time
    FROM producthunt_state
    WHERE uid=?
    """, (uid,))

    row = cur.fetchone()

    con.close()

    if row and row[0]:
        return row[0]

    return "1970-01-01T00:00:00Z"


def save_last_time(uid, ts):

    con = db()
    cur = con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO producthunt_state
    (uid, last_post_time)
    VALUES (?,?)
    """, (uid, ts))

    con.close()


# ---------------- HTTP ----------------

def safe_post(uid, query):

    try:

        r = requests.post(
            API_URL,
            json={"query": query},
            headers=get_headers(uid),
            timeout=15
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code == 429:
            time.sleep(60)

    except Exception:
        time.sleep(5)

    return None


# ---------------- Batch Inserts ----------------

def insert_posts(uid, posts):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    rows = []


    for p in posts:

        topics = [
            t["node"]["name"]
            for t in p.get("topics", {}).get("edges", [])
        ]

        makers = [
            m.get("name")
            for m in p.get("makers", [])
        ]


        rows.append((
            uid,

            p.get("id"),

            p.get("name"),
            p.get("tagline"),

            p.get("votesCount"),
            p.get("commentsCount"),

            p.get("createdAt"),
            p.get("url"),

            ",".join(topics),
            ",".join(makers),

            str(p),

            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO producthunt_posts (

        uid,

        post_id,

        name,
        tagline,

        votes,
        comments,

        created_at,
        url,

        topics,
        makers,

        raw_json,
        fetched_at
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)

    con.close()


def insert_topics(uid, topics):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    rows = []


    for t in topics:

        rows.append((
            uid,

            t.get("id"),

            t.get("name"),
            t.get("slug"),

            t.get("followersCount"),

            str(t),

            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO producthunt_topics (

        uid,

        topic_id,

        name,
        slug,

        followers,

        raw_json,
        fetched_at
    )
    VALUES (?,?,?,?,?,?,?)
    """, rows)

    con.close()


# ---------------- Main Sync ----------------
def sync_producthunt(uid, sync_type="incremental", limit=30):

    last_time = get_last_time(uid)

    rows_for_destination = []
    new_posts = []
    topics_inserted = 0

    # -------- POSTS --------
    posts_q = f"""
    {{
      posts(first: {limit}) {{
        nodes {{
          id
          name
          tagline
          votesCount
          commentsCount
          createdAt
          url
          topics {{
            edges {{
              node {{ name }}
            }}
          }}
          makers {{
            name
            username
          }}
        }}
      }}
    }}
    """

    posts_data = safe_post(uid,posts_q)

    if not posts_data:
        return {"rows": [], "posts": 0}

    posts = posts_data.get("data", {}) \
                      .get("posts", {}) \
                      .get("nodes", [])

    for p in posts:

        created = p.get("createdAt")

        if sync_type == "incremental" and created <= last_time:
            continue

        new_posts.append(p)

        rows_for_destination.append({
            "uid": uid,
            "post_id": p.get("id"),
            "name": p.get("name"),
            "tagline": p.get("tagline"),
            "votes": p.get("votesCount"),
            "comments": p.get("commentsCount"),
            "created_at": created,
            "url": p.get("url"),
            "topics": ",".join([
                t["node"]["name"]
                for t in p.get("topics", {}).get("edges", [])
            ]),
            "makers": ",".join([
                m.get("name")
                for m in p.get("makers", [])
            ])
        })

    if new_posts:
        insert_posts(uid, new_posts)
        latest = max(p["createdAt"] for p in new_posts)
        save_last_time(uid, latest)

    # -------- TOPICS --------
    topics_q = """
    {
      topics(first: 20) {
        nodes {
          id
          name
          slug
          followersCount
        }
      }
    }
    """

    topics_data = safe_post(topics_q)

    if topics_data:

        topics = topics_data.get("data", {}) \
                            .get("topics", {}) \
                            .get("nodes", [])

        if topics:
            insert_topics(uid, topics)
            topics_inserted = len(topics)

            for t in topics:
                rows_for_destination.append({
                    "uid": uid,
                    "topic_id": t.get("id"),
                    "topic_name": t.get("name"),
                    "slug": t.get("slug"),
                    "followers": t.get("followersCount")
                })

    return {
        "rows": rows_for_destination,
        "posts": len(new_posts),
        "topics": topics_inserted
    }